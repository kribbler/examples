<?php

namespace App\Http\Controllers\Api\Members;

use Validator;
use Illuminate\Http\Request;
use Illuminate\Http\Response;

use App\Http\Requests;
use App\Http\Requests\JobListRequest;
use App\Http\Requests\JobDetailsRequest;
use App\Http\Requests\JobAddRequest;
use App\Http\Requests\JobLegacyImportRequest;
use App\Http\Controllers\Controller;

use App\Helpers\CoreApi;
use App\Helpers\CoreDB;

use App\Models\Core\Job;
use App\Models\Core\Customer;

use Exception;
use App\Helpers\Lib\Api\ApiException;

use Log;
use DB;
use Cache;
use PHP_Timer;

class JobController extends Controller
{

    public function getDetails(JobDetailsRequest $request, $job_id)
    {
        $response = [];
        $status_code = 400;
        
        try {

            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemes($customer_id)->pluck('id');

            $validator = Validator::make([
                'job_id' => $job_id,
            ], [
                'job_id' => 'required|integer',
            ]);

            if ($validator->fails()) {
                throw new ApiException('Validation failed.');
            }

            $include_work_types    = $request->input('include_work_types') ?? 0;
            $include_account_items = $request->input('include_account_items') ?? 0;
            $include_policies      = $request->input('include_policies') ?? 0;
            $include_letters       = $request->input('include_letters') ?? 0;
            $include_audit_flags   = $request->input('include_audit_flags') ?? 0;

            $job = $this->getSingleJobQuery()
                ->where('jobs.id', $job_id)
                ->where(function ($query) use ($allowed_customers_scheme_ids) {
                    $query->whereIn('jobs.customers_scheme_id', $allowed_customers_scheme_ids)
                        ->orWhereIn('jobs.parent_customers_scheme_id', $allowed_customers_scheme_ids);
                })
                ->first();

            if (empty($job)) {
                throw new ApiException('Unable to find job.', 404);    
            }

            $job = CoreDB::toCakeArray($job);

            if ($include_account_items) {
                $account_items = $this->getAccountItemQuery($job['Job']['id'])->get();
                $job['total_account_items'] = count($account_items);
                $job['account_items'] = [];
                foreach ($account_items as $account_item) {
                    $job['account_items'][] = CoreDB::toCakeArray($account_item);
                }
            }

            if ($include_policies) {
                $policies = $this->getPolicyQuery($job['Job']['id'])->get();
                $job['total_policies'] = count($policies);
                $job['policies'] = [];
                foreach ($policies as $policy) {
                    $job['policies'][] = CoreDB::toCakeArray($policy);
                }
            }

            if ($include_letters) {
                $letters = $this->getLettersQuery($job['Job']['id'])->get();
                $job['total_letters'] = count($letters);
                $job['letters'] = [];
                foreach ($letters as $letter) {
                    $job['letters'][] = CoreDB::toCakeArray($letter);
                }
            }

            if ($include_work_types) {
                $work_types = $this->getWorkTypesQuery($job['Job']['id'])->get();
                $job['total_work_types'] = count($work_types);
                $job['work_types'] = [];
                foreach ($work_types as $work_type) {
                    $job['work_types'][] = CoreDB::toCakeArray($work_type);
                }
            }

            if ($include_audit_flags) {
                $audits_flags = $this->getAuditFlagQuery($job['Job']['id'])->get();
                $job['total_audits_flags'] = count($audits_flags);
                $job['audits_flags'] = [];
                foreach ($audits_flags as $audits_flag) {
                    $job['audits_flags'][] = CoreDB::toCakeArray($audits_flag);
                }
            }

            $response = $job;
            $status_code = 200;

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function getStats(Request $request)
    {
        $response = [];
        $status_code = 400;
        
        try {
            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemes($customer_id)->pluck('id');

            $query = $this->getJobStatsQuery()
                ->where(function ($query) use ($allowed_customers_scheme_ids) {
                    $query->whereIn('jobs.customers_scheme_id', $allowed_customers_scheme_ids)
                        ->orWhereIn('jobs.parent_customers_scheme_id', $allowed_customers_scheme_ids);
                });

            $jobs = $query->get();
            $total_records = count($jobs);

            if (empty($jobs)) {
                $status_code = 404;
                throw new ApiException('Unable to find jobs.');
            }

            $response['jobs'] = [];
            $response['total_records'] = $total_records;
            foreach ($jobs as $job) {
                $job = CoreDB::toCakeArray($job);

                $state = '';
                $crlm_customers_scheme_id = 17822;

                if ($job['Job']['customers_scheme_id'] == 17822) {
                    $state = 'Not Yet Assigned';
                }
                if (!$job['StateCompleted']['value']) {
                    $state = 'Awaiting Signoff';
                }

                if ($job['StateCompleted']['value']) {
                    $state = 'Awaiting Payment';
                }

                if ($job['JobPaidDate']['value']) {
                    $state = 'Policy Issued';
                }

                if ($job['Job']['void']) {
                    $state = 'Cancelled';
                }

                $response['jobs'][] = [
                    'id'        => $job['Job']['id'],
                    'paid_date' => $job['JobPaidDate']['value'],
                    'state'     => $state,
                ];
            }
            $status_code = 200;

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function signOff(Request $request, $job_id)
    {
        $response = [];
        $status_code = 400;
        
        try {

            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemes($customer_id)->pluck('id');

            $validator = Validator::make([
                'job_id' => $job_id,
            ], [
                'job_id' => 'required|integer',
            ]);

            if ($validator->fails()) {
                throw new ApiException('Validation failed.');
            }

            $job = $this->getJobQuery()
                ->where('jobs.id', $job_id)
                ->where(function ($query) use ($allowed_customers_scheme_ids) {
                    $query->whereIn('jobs.customers_scheme_id', $allowed_customers_scheme_ids)
                        ->orWhereIn('jobs.parent_customers_scheme_id', $allowed_customers_scheme_ids);
                })
                ->first();

            if (empty($job)) {
                throw new ApiException('Unable to find job.', 404);    
            }

            $core_api = CoreApi::queryApi([
                'url' => '/MembersApi/signOffJob.json',
                'parameters' => [
                    'job_id'         => $job_id,
                    'compile_charge' => true,
                ],
            ]);
            $response = $core_api['response'];
            $status_code = $core_api['response_status'] ?? 400;

            if ($status_code != 200) {
                throw new Exception('Unable to query Core API: ' . $response['error']);
            }

            $success = $response->success ?? false;

            if (!$success) {
                $status_code = 400;
            }

            $status_code = 200;

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function edit(JobAddRequest $request, $job_id)
    {
        $response = [];
        $status_code = 400;
        
        try {

            $validator = Validator::make([
                'job_id' => $job_id,
            ], [
                'job_id' => 'required|integer',
            ]);

            if ($validator->fails()) {
                throw new ApiException('Validation failed.');
            }

            $customer_id = $request->get('customer_id');
            $customers_scheme_id = Job::find($job_id)->customers_scheme_id ?? null;
            $parent_customers_scheme_id = Job::find($job_id)->parent_customers_scheme_id ?? null;
            $allowed_customers_scheme_ids = Customer::getAllowedActiveCustomersSchemes($customer_id)->pluck('id')->toArray();

            $job = $request->input('job');

            if (!in_array($customers_scheme_id, $allowed_customers_scheme_ids) && !in_array($parent_customers_scheme_id, $allowed_customers_scheme_ids)) {
                Log::warning('Invalid scheme selection. ' . $customers_scheme_id . ' is not in ' . implode(',', $allowed_customers_scheme_ids) . ' for job ' . $job_id);
                throw new ApiException('Invalid scheme selection:');
            }

            if (is_array($job)) {
                $job['Job']['id'] = $job_id;
                $job['Job']['customers_scheme_id'] = $customers_scheme_id;
            } else {
                $job->Job->id = $job_id;
                $job->Job->customers_scheme_id = $customers_scheme_id;
            }

            $core_api = CoreApi::queryApi([
                'url' => '/MembersApi/reviseJob.json',
                'parameters' => [
                    'job'         => $job,
                    'customer_id' => $customer_id,
                ],
            ]);
            $response = $core_api['response'];
            $status_code = $core_api['response_status'] ?? 400;

            if ($status_code != 200) {
                throw new Exception('Unable to query Core API: ' . $response['error']);
            }

            $success = $response->success ?? false;

            if (!$success) {
                $status_code = 400;
                Log::info('Revision failed: ' . $response->error);
            }

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function add(JobAddRequest $request)
    {
        $response = [];
        $status_code = 400;
        
        try {

            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedActiveCustomersSchemes($customer_id)->pluck('id')->toArray();

            $job = $request->input('job');
            
            if (is_array($job)) {
                $customers_scheme_id = $job['Job']['customers_scheme_id'];
                $parent_customers_scheme_id = $job['Job']['parent_customers_scheme_id'] ?? null;
            } else {
                $customers_scheme_id = $job->Job->customers_scheme_id;
                $parent_customers_scheme_id = $job->Job->parent_customers_scheme_id ?? null;
            }

            if (!in_array($customers_scheme_id, $allowed_customers_scheme_ids) && !in_array($parent_customers_scheme_id, $allowed_customers_scheme_ids)) {
                throw new ApiException('Invalid scheme selection ' . $customers_scheme_id . json_encode($allowed_customers_scheme_ids));
            }
            $core_api = CoreApi::queryApi([
                'url' => '/MembersApi/addJob.json',
                'parameters' => [
                    'job'         => $request->input('job'),
                    'customer_id' => $customer_id,
                    'action'      => 'add',
                ],
            ]);
            $response = $core_api['response'];
            $status_code = $core_api['response_status'] ?? 400;

            if ($status_code != 200) {
                throw new Exception('Unable to query Core API: ' . $response['error']);
            }

            $success = $response->success ?? false;

            if (!$success) {
                $status_code = 400;
            }

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function legacyImport(JobLegacyImportRequest $request)
    {
        $response = [];
        $status_code = 400;
        
        try {
            $customer_id = $request->get('customer_id');
            $allowed_customers_schemes = Customer::getAllowedActiveCustomersSchemes($customer_id);
            $allowed_customers_scheme_ids = $allowed_customers_schemes->pluck('id')->toArray();

            if (!in_array($request->input('customers_scheme_id'), $allowed_customers_scheme_ids)) {
                throw new ApiException('Invalid scheme selection:');
            }

            $scheme_id = $allowed_customers_schemes->where('id', $request->input('customers_scheme_id'))->pop()->scheme_id;
            $core_api = CoreApi::queryApi([
                'url' => '/JobImportsApi/import.json',
                'parameters' => [
                    'import_data'         => $request->input('import_data'),
                    'customers_scheme_id' => $request->input('customers_scheme_id'),
                    'scheme_id'           => $scheme_id,
                ],
            ]);

            $response = $core_api['response'];
            $status_code = $core_api['response_status'] ?? 400;

            if ($status_code != 200) {
                throw new Exception('Unable to query Core API: ' . $response['error']);
            }

            $success = $response->success ?? false;

            if (!$success) {
                $status_code = 400;
            }

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function getList(JobListRequest $request)
    {
        $response = [];
        $status_code = 400;
        
        try {

            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemes($customer_id)->pluck('id');

            $query = $this->getJobQuery()
                ->orderBy('jobs.id', 'DESC')
                ->where(function ($query) use ($allowed_customers_scheme_ids) {
                    $query->whereIn('jobs.customers_scheme_id', $allowed_customers_scheme_ids)
                        ->orWhereIn('jobs.parent_customers_scheme_id', $allowed_customers_scheme_ids);
                });

            $include_account_items = $request->input('include_account_items') ?? 0;
            $include_policies      = $request->input('include_policies') ?? 0;
            $include_letters       = $request->input('include_letters') ?? 0;
            $include_work_types    = $request->input('include_work_types') ?? 0;
            $include_has_letters   = $request->input('include_has_letters') ?? 0;
            $include_has_policies  = $request->input('include_has_policies') ?? 0;
            if ($request->input('job_id')) {
                $query->where('jobs.id', $request->input('job_id'));
            }
            if ($request->input('job_reference')) {
                $query->where('jobs.reference', $request->input('job_reference'));
            }
            if ($request->input('job_last_name_or_property_owner')) {
                $query->where('jobs.last_name_or_property_owner', $request->input('job_last_name_or_property_owner'));
            }
            if ($request->input('job_status')) {
                switch ($request->input('job_status')) {
                    case 'Awaiting Sign-off' :
                        $query->having('state_completed', '=', '0');
                        break;
                    case 'Completed':
                        $query->having('state_completed', '=', '1');
                        break;
                    case 'Awaiting Sign-off and Post Est Completed':
                        $date_now = new DateTime(date('Y-m-d'), new DateTimeZone('UTC'));
                        $date_now->format('U');
                        $query->having('state_completed', '=', '0')
                            ->whereRaw('[UNIX_TIMESTAMP(DATE_FORMAT(job.completion_estimated, "%Y-%m-%d")) < ' . $date_now)
                            ->orWhereNull('job.completion_estimated');
                        break;
                }
            }
            if ($request->input('customers_scheme_id')) {
                $query->where('jobs.customers_scheme_id', $request->input('customers_scheme_id'));
            }

            if ($request->input('email_address')) {
                $query->where('jobs.email_address', $request->input('email_address'));
            }

            if ($request->input('telephone_no')) {
                $query->where('jobs.telephone_no', $request->input('telephone_no'));
            }

            if ($request->input('address')) {
                $query->where(function($query) use ($request) {
                    $query->where('jobs.address_1', 'LIKE', '%' . $request->input('address') . '%')
                        ->orWhere('jobs.address_2', 'LIKE', '%' . $request->input('address') . '%')
                        ->orWhere('jobs.address_3', 'LIKE', '%' . $request->input('address') . '%')
                        ->orWhere('jobs.install_address_1', 'LIKE', '%' . $request->input('address') . '%')
                        ->orWhere('jobs.install_address_2', 'LIKE', '%' . $request->input('address') . '%')
                        ->orWhere('jobs.install_address_3', 'LIKE', '%' . $request->input('address') . '%');
                });
            }

            if ($request->input('postcode')) {
                $query->where(function($query) use ($request) {
                    $query->where('jobs.address_postcode', 'LIKE', '%' . $request->input('postcode') . '%')
                        ->orWhere('jobs.address_postcode', 'LIKE', '%' . str_ireplace(" ", "", $request->input('postcode')) . '%')
                        ->orWhere('jobs.install_address_postcode', 'LIKE', '%' . $request->input('postcode') . '%')
                        ->orWhere('jobs.install_address_postcode', 'LIKE', '%' . str_ireplace(" ", "", $request->input('postcode')) . '%');
                });
            }
            
            // if state_completed in the json is 0 then it doesn't filter.
            if ($request->input('state_completed') !== null) {
                $query->having('state_completed', '=', $request->input('state_completed'));
            }

            $total_jobs = $query->get();
            $total_records = count($total_jobs);

            $offset = $request->input('offset') ?? 0;
            $limit = $request->input('limit') ?? 50;

            $jobs = $total_jobs->slice($offset, $limit);

            if (empty($jobs)) {
                $status_code = 404;
                throw new ApiException('Unable to find jobs.');    
            }

            $response['total_records'] = $total_records;
            $response['returned_records'] = count($jobs);
            $response['limit'] = $limit;
            $response['results'] = [];
            foreach ($jobs as $job) {
                $job = CoreDB::toCakeArray($job);
                $job = $this->calculateJobStatus($job);

                if ($include_has_policies) {
                    $job['has_policies'] = (Bool) $this->hasPolicies($job['Job']['id']);
                }

                if ($include_has_letters) {
                    $job['has_letters'] = (Bool) $this->hasLetters($job['Job']['id']);
                }

                if ($include_policies) {
                    $policies = $this->getPolicyQuery($job['Job']['id'])->get();
                    $job['total_policies'] = count($policies);
                    $job['policies'] = [];
                    foreach ($policies as $policy) {
                        $job['policies'][] = CoreDB::toCakeArray($policy);
                    }
                }

                if ($include_letters) {
                    $letters = $this->getLettersQuery($job['Job']['id'])->get();
                    $job['total_letters'] = count($letters);
                    $job['letters'] = [];
                    foreach ($letters as $letter) {
                        $job['letters'][] = CoreDB::toCakeArray($letter);
                    }
                }

                if ($include_account_items) {
                    $premium_amount = 0;
                    $premium_amount_inc_tax = 0;

                    $account_items = $this->getAccountItemQuery($job['Job']['id'])->get();
                    $job['total_account_items'] = count($account_items);
                    $job['account_items'] = [];
                    foreach ($account_items as $account_item) {
                        $account_item = CoreDB::toCakeArray($account_item);
                        $job['account_items'][] = $account_item;
                        $premium_amount += $account_item['AccountItem']['amount'] ?? 0;
                        $premium_amount_inc_tax += $account_item['AmountIncTax']['value'] ?? 0;
                    }

                    $job['premium_amount'] = $premium_amount;
                    $job['premium_amount_inc_tax'] = $premium_amount_inc_tax;
                }

                if ($include_work_types) {
                    $work_types = $this->getWorkTypesQuery($job['Job']['id'])->get();
                    $job['total_work_types'] = count($work_types);
                    $job['work_types'] = [];
                    foreach ($work_types as $work_type) {
                        $job['work_types'][] = CoreDB::toCakeArray($work_type);
                    }
                }

                $response['results'][] = $job;
            }
            $status_code = 200;

        } catch (ApiException $e) {
            Log::error($e->getMessage());
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            Log::error($e->getMessage());
            if ($status_code == 200) {
                $status_code = 500;
            }
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    private function calculateJobStatus($job)
    {
        $job['Job']['flags'] = $this->getReviewRequired($job['Job']['id'], false, true);
        
        $underwriting_review = false;
        foreach ($job['Job']['flags'] as $flag) {
            if (isset($flag['AuditsFlagsReason']) && $flag['AuditsFlagsReason']['require_underwriting_review'] && $flag['AuditsFlag']['audits_flags_stage_id'] != 300) {
                $underwriting_review = true;
            }
        }
        $job['calculated_states'] = [];
        if (!$underwriting_review) {
            if ($job['StateCompleted']['value']) {
                $job['calculated_states'][] = 'completed';
            } else {
                $job['calculated_states'][] = 'awaiting-sign-off';
            }
        } 

        if ($job['AuditStageId']['value'] != 0) {
            $job['calculated_states'][] = 'under-review';
        }

        if ($underwriting_review) {
            $job['calculated_states'][] = 'underwriting-review';
        }
        $job['underwriting_review'] = $underwriting_review;
        return $job;
    }

    private function getReviewRequired($job_id, $pullSuccessful = false, $showFileCount = false)
    {
        $calculated_state = null;
        $audits_flags_query_fields = [
            'audits_flags' => [
                'fields' => [
                    'id', 'audits_flags_stage_id', 
                ],
            ],
            'audits_flags_reasons' => [
                'fields' => [
                    'name', 'require_underwriting_review',
                ],
            ],
            'audits_flags_items' => [
                'fields' => [
                    'id', 'job_id',
                ],
            ],
        ];

        if ($showFileCount) {
            $audits_flags_query_fields[] = '(' . 'SELECT count(*) FROM uploaded_files UF WHERE UF.audits_flag_id = AuditsFlag.id AND UF.void = 0' . ') as file_count';
        }

        $audits_flags_query = DB::connection('coredb')
            ->table('audits_flags_items')
            ->leftJoin('audits_flags', 'audits_flags.id', '=', 'audits_flags_items.audits_flag_id')
            ->leftJoin('audits_flags_reasons', 'audits_flags_reasons.id', '=', 'audits_flags.audits_flags_reason_id')
            ->where('audits_flags_items.job_id', $job_id)
            ->where('audits_flags.void', 0)
            ->select(CoreDB::prefixColumns($audits_flags_query_fields));

        if ($pullSuccessful) {
            $audits_flags_query = $audits_flags_query
                ->where('audits_flags.audits_flags_stage_id', '!=', '300');
        }
        
        $audits_flags = $audits_flags_query->get();
        $job['total_audits_flags'] = count($audits_flags);
        $job['audits_flags'] = [];
        foreach ($audits_flags as $audits_flag) {
            $job['audits_flags'][] = CoreDB::toCakeArray($audits_flag);
        }
        return $job;
    }

    private function getAuditFlagQuery($job_id)
    {
        return DB::connection('coredb')
            ->table('audits_flags_items')
            ->leftJoin('audits_flags', 'audits_flags.id', '=', 'audits_flags_items.audits_flag_id')
            ->leftJoin('audits_flags_reasons', 'audits_flags_reasons.id', '=', 'audits_flags.audits_flags_reason_id')
            ->where('audits_flags_items.job_id', $job_id)
            ->select(CoreDB::prefixColumns([
                'audits_flags' => [
                    'fields' => [
                        'id', 'audits_flags_stage_id', 
                    ],
                ],
                'audits_flags_reasons' => [
                    'fields' => [
                        'name', 'require_underwriting_review',
                    ],
                ],
                'audits_flags_items' => [
                    'fields' => [
                        'id', 'job_id',
                    ],
                ],
            ]));
    }

    private function getAccountItemQuery($job_id)
    {
        return DB::connection('coredb')
            ->table('account_items')
            ->where('relation_id', $job_id)
            ->where('account_relation_type_id', 'jobs_contractor')
            ->where('void', 0)
            ->where('cancelled', 0)
            ->where('refunded', 0)
            ->select(array_merge([DB::raw('ROUND(account_items.amount + ( (account_items.amount / 100) * (SELECT value FROM taxes WHERE taxes.id = account_items.tax_id) ), 2) as amount_inc_tax')]
                ,
                CoreDB::prefixColumns([
                'account_items' => [
                    'fields' => [
                        'id','account_fee_type_id','amount','policy_type_id','amount_pay_insurer','tax_id','account_payment_type_id','account_relation_type_id','relation_id','account_charge_id','void','refunded','cancelled','created'
                    ],
                ],
            ])));
    }

    private function hasLetters($job_id)
    {
        return DB::connection('coredb')
            ->table('jobs_letters')
            ->leftJoin('jobs', 'jobs_letters.job_id', '=', 'jobs.id')
            ->where('jobs_letters.void', 0)
            ->where('jobs.id', $job_id)
            ->count();
    }

    private function hasPolicies($job_id)
    {
        return DB::connection('coredb')
            ->table('policies')
            ->leftJoin('account_items', 'account_items.id', '=', 'policies.account_item_id')
            ->leftJoin('jobs', 'jobs.id', '=', 'account_items.relation_id')
            ->orderBy('policies.created', 'DESC')
            ->where('jobs.id', $job_id)
            ->where('policies.void', 0)
            ->count();
    }
    
    private function getLettersQuery($job_id)
    {
        return DB::connection('coredb')
            ->table('jobs_letters')
            ->leftJoin('jobs', 'jobs_letters.job_id', '=', 'jobs.id')
            ->where('jobs_letters.void', 0)
            ->where('jobs.id', $job_id)
            ->select(CoreDB::prefixColumns([
                'jobs_letters' => ['fields' => []],
                'jobs' => ['fields' => ['last_name_or_property_owner', 'address_1', 'address_postcode', 'install_address_1', 'install_address_postcode']],
            ]));
    }

    private function getPolicyQuery($job_id)
    {
        return DB::connection('coredb')
            ->table('policies')
            ->leftJoin('schemes', 'schemes.id', '=', 'policies.scheme_id')
            ->leftJoin('policy_types', 'policy_types.id', '=', 'policies.policy_type_id')
            ->leftJoin('account_items', 'account_items.id', '=', 'policies.account_item_id')
            ->leftJoin('jobs', 'jobs.id', '=', 'account_items.relation_id')
            ->leftJoin('customers_schemes', 'customers_schemes.id', '=', 'jobs.customers_scheme_id')
            ->leftJoin('customers', 'customers.id', '=', 'customers_schemes.customer_id')
            ->orderBy('policies.created', 'DESC')
            ->where('jobs.id', $job_id)
            ->where('policies.void', 0)
            ->select(
                CoreDB::prefixColumns([
                    'policies' => ['fields' => ['id', 'scheme_id', 'policy_id', 'account_item_id', 'policy_type_id', 'void', 'created']],
                    'schemes' => ['fields' => ['id', 'name', 'prefix', 'policy_prefix']],
                    'jobs' => ['fields' => ['id']],
                ])
            );
    }

    private function getWorkTypesQuery($job_id)
    {
        return DB::connection('coredb')
            ->table('jobs_work_types')
            ->leftJoin('work_types', 'jobs_work_types.work_type_id', '=', 'work_types.id')
            ->where('jobs_work_types.job_id', $job_id)
            ->where('work_types.void', 0)
            ->select(CoreDB::prefixColumns([
                'work_types' => [
                    'fields' => [
                        'id', 'name', 'short_name', 
                    ],
                ],
            ]));
    }

    private function getSingleJobQuery()
    {
        return DB::connection('coredb')
            ->table('jobs')
            // ->leftJoin('jobs_custom_eco_entries', 'jobs_custom_eco_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_eco_new_entries', 'jobs_custom_eco_new_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_cgs_entries', 'jobs_custom_cgs_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_gpt_entries', 'jobs_custom_gpt_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_knot_entries', 'jobs_custom_knot_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_knot_entries_new', 'jobs_custom_knot_entries_new.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_dawwi_entries', 'jobs_custom_dawwi_entries.id', '=', 'jobs.id')
            ->leftJoin('titles', 'titles.id', '=', 'jobs.title_id')
            ->leftJoin('jobs_states', 'jobs_states.job_id', '=', 'jobs.id')
            ->leftJoin('customers_schemes', 'jobs.customers_scheme_id', '=', 'customers_schemes.id')
            ->leftJoin('schemes', 'customers_schemes.scheme_id', '=', 'schemes.id')
            ->groupBy('jobs.id')
            ->select(array_merge(
                [DB::raw('(IF((SELECT jobs_revisions.id FROM jobs_revisions AS jobs_revisions WHERE jobs_revisions.job_id = jobs.id AND jobs_revisions.void = 0 AND jobs_revisions.applied = 0 LIMIT 1),1,0)) as revision_pending')],
                [DB::raw('(IF((SELECT jobs_states.id FROM jobs_states AS jobs_states WHERE jobs_states.job_id = jobs.id AND jobs_states.resolved IS NOT NULL AND jobs_states.void = 0 AND jobs_states.state_id = 40000 LIMIT 1),1,0)) as state_completed')],
                [DB::raw('(SELECT IF(IFNULL(sum(audits_flag.audits_flags_stage_id), 0), audits_flag.audits_flags_stage_id, 0) FROM audits_flags_items AS audits_flags_item LEFT JOIN audits_flags AS audits_flag ON audits_flag.id = audits_flags_item.audits_flag_id WHERE audits_flags_item.job_id = jobs.id AND audits_flag.void = 0 AND audits_flag.audits_flags_stage_id IN(100,101,200) AND audits_flag.audits_flags_reason_id IN(300,400,603)) as audit_stage_id')],
                CoreDB::prefixColumns([
                    'jobs' => [
                        'fields' => [
                            'id', 'term', 'contract_value', 'deposit_cover', 'deposit_paid', 'deposit_amount', 'completion', 'address_1', 'address_2', 'address_3', 'address_postcode', 'install_address_differs', 'install_address_1', 'install_address_2', 'install_address_3', 'install_address_postcode', 'title_id', 'first_name', 'last_name_or_property_owner', 'email_address', 'telephone_no', 'modified', 'customers_scheme_id', 'rate_id', 'competent_persons_scheme_id', 'created', 'reference'
                        ],
                    ],
                    // 'jobs_custom_eco_entries' => ['fields' => []],
                    // 'jobs_custom_eco_new_entries' => ['fields' => []],
                    // 'jobs_custom_cgs_entries' => ['fields' => []],
                    // 'jobs_custom_gpt_entries' => ['fields' => []],
                    // 'jobs_custom_knot_entries' => ['fields' => []],
                    // 'jobs_custom_knot_entries_new' => ['fields' => []],
                    // 'jobs_custom_dawwi_entries' => ['fields' => []],
                    'titles' => ['fields' => ['name']],
                    'customers_schemes' => ['fields' => ['scheme_id']],
                    'schemes' => ['fields' => ['id', 'name']],
                ])
            ));
    }

    private function getJobQuery()
    {
        return DB::connection('coredb')
            ->table('jobs')
            // ->leftJoin('jobs_custom_eco_entries', 'jobs_custom_eco_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_eco_new_entries', 'jobs_custom_eco_new_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_cgs_entries', 'jobs_custom_cgs_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_gpt_entries', 'jobs_custom_gpt_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_knot_entries', 'jobs_custom_knot_entries.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_knot_entries_new', 'jobs_custom_knot_entries_new.id', '=', 'jobs.id')
            // ->leftJoin('jobs_custom_dawwi_entries', 'jobs_custom_dawwi_entries.id', '=', 'jobs.id')
            ->leftJoin('titles', 'titles.id', '=', 'jobs.title_id')
            ->leftJoin('jobs_states', 'jobs_states.job_id', '=', 'jobs.id')
            ->groupBy('jobs.id')
            ->select(array_merge(
                [DB::raw('(IF((SELECT jobs_revisions.id FROM jobs_revisions AS jobs_revisions WHERE jobs_revisions.job_id = jobs.id AND jobs_revisions.void = 0 AND jobs_revisions.applied = 0 LIMIT 1),1,0)) as revision_pending')],
                [DB::raw('(IF((SELECT jobs_states.id FROM jobs_states AS jobs_states WHERE jobs_states.job_id = jobs.id AND jobs_states.resolved IS NOT NULL AND jobs_states.void = 0 AND jobs_states.state_id = 40000 LIMIT 1),1,0)) as state_completed')],
                [DB::raw('(SELECT IF(IFNULL(sum(audits_flag.audits_flags_stage_id), 0), audits_flag.audits_flags_stage_id, 0) FROM audits_flags_items AS audits_flags_item LEFT JOIN audits_flags AS audits_flag ON audits_flag.id = audits_flags_item.audits_flag_id WHERE audits_flags_item.job_id = jobs.id AND audits_flag.void = 0 AND audits_flag.audits_flags_stage_id IN(100,101,200) AND audits_flag.audits_flags_reason_id IN(300,400,603)) as audit_stage_id')],
                CoreDB::prefixColumns([
                    'jobs' => [
                        'fields' => [
                            'id', 'term', 'contract_value', 'deposit_cover', 'deposit_paid', 'deposit_amount', 'completion', 'address_1', 'address_2', 'address_3', 'address_postcode', 'install_address_differs', 'install_address_1', 'install_address_2', 'install_address_3', 'install_address_postcode', 'title_id', 'first_name', 'last_name_or_property_owner', 'email_address', 'telephone_no', 'modified', 'customers_scheme_id', 'rate_id', 'competent_persons_scheme_id', 'created', 'reference'
                        ],
                    ],
                    // 'jobs_custom_eco_entries' => ['fields' => []],
                    // 'jobs_custom_eco_new_entries' => ['fields' => []],
                    // 'jobs_custom_cgs_entries' => ['fields' => []],
                    // 'jobs_custom_gpt_entries' => ['fields' => []],
                    // 'jobs_custom_knot_entries' => ['fields' => []],
                    // 'jobs_custom_knot_entries_new' => ['fields' => []],
                    // 'jobs_custom_dawwi_entries' => ['fields' => []],
                    'titles' => ['fields' => ['name']],
                ])
            ));
    }

    private function getJobStatsQuery()
    {
        return DB::connection('coredb')
            ->table('jobs')
            ->leftJoin('jobs_states', 'jobs_states.job_id', '=', 'jobs.id')
            ->groupBy('jobs.id')
            ->select(array_merge(
                CoreDB::prefixColumns([
                    'jobs' => [
                        'fields' => ['id', 'customers_scheme_id', 'void'],
                    ],
                ]),
                [DB::raw("
                    (SELECT account_invoices.created FROM account_invoices AS account_invoices
                    LEFT JOIN account_charges ON account_invoices.account_charge_id = account_charges.id
                    LEFT JOIN account_items ON account_items.account_charge_id = account_charges.id
                    WHERE account_items.account_relation_type_id = 'jobs_contractor'
                    AND account_items.relation_id = jobs.id
                    AND account_items.void = 0
                    AND account_items.cancelled = 0
                    AND account_items.refunded = 0
                    AND account_charges.bounced = 0
                    AND account_charges.void = 0
                    AND jobs.void = 0
                    LIMIT 1) as job_paid_date
                ")],
                [DB::raw('(IF((SELECT jobs_states.id FROM jobs_states AS jobs_states WHERE jobs_states.job_id = jobs.id AND jobs_states.resolved IS NOT NULL AND jobs_states.void = 0 AND jobs_states.state_id = 40000 LIMIT 1),1,0)) as state_completed')]
            ));
    }

}
