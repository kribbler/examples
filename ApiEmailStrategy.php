<?php

namespace Customer\Bundle\CrmBundle\Email;

use Doctrine\Common\Persistence\ManagerRegistry;
use Customer\Bundle\CrmBundle\Email\Converter\CrmEmailParameterConverter;
use Customer\Bundle\CrmBundle\Exception\CannotExtractCrmIdException;
use Customer\Bundle\CrmBundle\Model\ValidationWarningConstraintsTrait;
use Customer\Bundle\CrmBundle\Model\Write\ValidateRequestTrait;
use Customer\Bundle\GibIntegrationBundle\Exception\ApiInteractionException;
use Customer\Bundle\GibIntegrationBundle\CustomerApiClient\Channels;
use Customer\Bundle\GibIntegrationBundle\CustomerApiClient\Client\CustomerApiClientInterface;
use Customer\Bundle\GibIntegrationBundle\CustomerApiClient\MarketNameProvider;
use Customer\Bundle\GibIntegrationBundle\CustomerApiClient\Request;
use Customer\Bundle\GibIntegrationBundle\CustomerApiClient\Schemas;
use Customer\Bundle\GibIntegrationBundle\CustomerApiClient\Tags;
use Core\Bundle\MailBundle\Entity\EmailEvent;
use Core\Component\Email\Email;
use Core\Component\Email\EmailInterface;
use Core\Component\Email\Strategy\Strategy;
use Core\Util\Doctrine\AccessEntityManagerFromDoctrineTrait;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use Psr\Log\NullLogger;

class CustomerApiEmailStrategy extends Strategy
{
    use AccessEntityManagerFromDoctrineTrait;
    use ValidateRequestTrait;
    use ValidationWarningConstraintsTrait;

    const SYSTEM_NAME = 'Customerapi_transactional';

    /**
     * @var CustomerApiClientInterface
     */
    private $apiClient;

    /**
     * @var CrmIdPluckerInterface
     */
    private $crmIdPlucker;

    /**
     * @var array
     */
    private $emailNameMap;

    /**
     * @var bool
     */
    private $allowValidationFailures;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var MarketNameProvider
     */
    private $marketNameProvider;

    /**
     * @var CrmEmailParameterConverter
     */
    private $emailParameterConverter;

    /**
     * @var ManagerRegistry
     */
    private $doctrine;

    public function __construct(
        CustomerApiClientInterface $apiClient,
        CrmIdPluckerInterface $crmIdPlucker,
        MarketNameProvider $marketNameProvider,
        CrmEmailParameterConverter $emailParameterConverter,
        ManagerRegistry $doctrine,
        array $emailNameMap = [],
        bool $allowValidationFailures = true,
        ?LoggerInterface $logger = null
    ) {
        $this->apiClient = $apiClient;
        $this->crmIdPlucker = $crmIdPlucker;
        $this->emailNameMap = $emailNameMap;
        $this->allowValidationFailures = $allowValidationFailures;
        $this->logger = $logger ?: new NullLogger();
        $this->marketNameProvider = $marketNameProvider;
        $this->emailParameterConverter = $emailParameterConverter;
        $this->doctrine = $doctrine;
    }

    /**
     * @param EmailInterface $email
     *
     * @return void
     * @throws \Exception
     */
    public function doEmail(EmailInterface $email)
    {
        $parameters = $email->getParameters();
        $requiredParameters = ['market', 'locale'];
        if (array_diff($requiredParameters, array_keys($parameters))) {
            throw new \RuntimeException(
                sprintf('All emails must have market and locale parameters, and the email "%s" does not.', $email->getName())
            );
        }

        if (!$email instanceof Email) {
            throw new \Exception(sprintf('Cannot apply converted parameters to an object which is not an instance of PHX Email, but %s', get_class($email)));
        }

        $convertedParameters = $this->emailParameterConverter->convert($email->getParameters(), $email->getRecipient());
        $email->setParameters($convertedParameters);

        try {
            $crmId = $this->crmIdPlucker->pluckCrmIdFromData($convertedParameters);
        } catch (CannotExtractCrmIdException $e) {
            throw new \InvalidArgumentException(
                sprintf('Unable to establish which CRM ID to use for email "%s".', $email->getName())
            );
        }

        $success = false;
        $message = '';
        $aliasMessage = '';
        $logData = [];
        $calculatedEmailNameForSending = $this->getResolvedEmailName($email->getName());
        $isUsingMapping = $calculatedEmailNameForSending !== $email->getName();
        try {
            $aliasMessage = ($isUsingMapping)
                ? sprintf(' (mapped to "%s")', $calculatedEmailNameForSending)
                : '';
            $message = sprintf(
                'Sending email "%s"%s for customer ID %s attempted.',
                $email->getName(),
                $aliasMessage,
                $crmId
            );
            $payload = (object) [
                'eventType'  => $this->calculateApiNameForEmail($email),
                'attributes' => $parameters,
            ];
            $validation = $this->validateRequest(
                $payload,
                Schemas::CONSUMER_EMAIL,
                $this->getWarningConstraints()
            );
            if ($validation->isFail() && !$this->allowValidationFailures) {
                $this->logger->error(
                    'Payload for sending an email was invalid.',
                    array_merge(
                        [
                            'consumerId' => $crmId,
                        ],
                        $this->emitLogContextDataForValidationResult($validation)
                    )
                );

                return;
            }

            $response = $this->apiClient->send(
                (new Request())
                    ->setEndpoint(sprintf('/consumers/%s/email', $crmId))
                    ->setHttpMethod('POST')
                    ->setChannel(Channels::ECOM)
                    ->setMarket($parameters['market'])
                    ->setData($payload)
                    ->addTag(Tags::CRM)
                    ->setLabel('send_email')
            );

            if ($response->getStatusCode() === 201) {
                $success = true;
                $message = sprintf(
                    'Sent email "%s"%s successful for CRM ID %s.',
                    $email->getName(),
                    $aliasMessage ?? '',
                    $crmId
                );
            } else {
                $message = sprintf(
                    'Sending an email through the CRM failed. Response status was %s %s.',
                    $response->getStatusCode(),
                    $response->getReasonPhrase()
                );
                $logData = $this->buildLogData($email, $crmId, $aliasMessage);
            }
        } catch (ApiInteractionException $e) {
            $message = sprintf(
                'API interaction error sending an email to the Customer API: "%s"',
                $e->getMessage()
            );
            $logData = $this->buildLogData($email, $crmId, $aliasMessage);
        } catch (\Exception $e) {
            $message = sprintf(
                'Error sending an email (CRM) to the Customer API: "%s"',
                $e->getMessage()
            );
            $logData = $this->buildLogData($email, $crmId, $aliasMessage);
        } finally {
            $responseData = (isset($response) && $response instanceof ResponseInterface)
                ? json_decode((string) $response->getBody(), true)
                : null;
            if (json_last_error() !== JSON_ERROR_NONE) {
                $responseData = [
                    (isset($response) && $response instanceof ResponseInterface) ? (string) $response->getBody() : 'no response',
                ];
            }
            $this->logger
                ->log(
                    ($success ?? false) ? LogLevel::INFO : LogLevel::ERROR,
                    $message,
                    array_merge(
                        array_filter([
                            'response' => $responseData,
                            'emailParameters' => $email->getParameterReferences(),
                        ]),
                        $logData,
                        (isset($validation)) ? $this->emitLogContextDataForValidationResult($validation) : []
                    )
                );

                $this->logEvent($email, $message, $success);
                
                
            if ($success === false) {
                throw new \Exception($message);
            }
        }
    }

    /**
     * @inheritDoc
     */
    public function isTestable()
    {
        return true;
    }

    public function addEmailNameMappings(array $emailNameMappings): self
    {
        foreach ($emailNameMappings as $mappedFrom => $mappedTo) {
            $this->emailNameMap[$mappedFrom] = $mappedTo;
        }

        return $this;
    }

    private function calculateApiNameForEmail(EmailInterface $email): string
    {
        //if the email name is e.g. 'order_created', then expected format for event would be 'Core_OrderCreated_SE'

        $templateEventName = '';
        foreach (explode('_', $this->getResolvedEmailName($email->getName())) as $word) {
            $templateEventName .= ucfirst($word);
        }

        $parameters = $email->getParameters();

        return sprintf(
            'Core_%s_%s',
            $templateEventName,
            $this->getResolvedMarketName($parameters['market'])
        );
    }

    /**
     * Gets the name to use for the given email, taking into account any mappings defined.
     *
     * @param string $emailName
     *
     * @return string
     */
    private function getResolvedEmailName(string $emailName): string
    {
        if (array_key_exists($emailName, $this->emailNameMap)) {
            return $this->emailNameMap[$emailName];
        }

        return $emailName;
    }

    private function getResolvedMarketName(string $marketName): string
    {
        $marketName = strtolower($marketName);

        $marketName = $this->marketNameProvider->getName($marketName);

        return strtoupper($marketName);
    }

    private function buildLogData(EmailInterface $email, string $crmId, string $aliasMessage): array
    {
        return [
            'email' => sprintf(
                '"%s"%s',
                $email->getName(),
                $aliasMessage ?? ''
            ),
            'consumerId' => $crmId,
        ];
    }

    /**
     * @param EmailInterface $email
     * @param null|string $additionalData
     * @param null|bool $success
     */
    private function logEvent(EmailInterface $email, ?string $additionalData = null, ?bool $success = false)
    {
        $event = new EmailEvent();

        $event->setEmail($email->getRecipient());
        $event->setEventTime(new \DateTime());
        $event->setCategory($email->getName());

        if ($success) {
            $event->setEvent('sent');
            $event->setReference(json_encode($email->getParameterReferences()));
        } else {
            $event->setEvent('failed');
            $event->setReference(json_encode($email->getParameterReferences()));
        }

        $event->setParameters(json_encode($email->getParameterReferences()));
        $event->setSystem(self::SYSTEM_NAME);

        if ($additionalData) {
            $event->setAdditionalData($additionalData);
        }
        $this->getEntityManager()->persist($event);
        $this->getEntityManager()->flush();
    }
}
