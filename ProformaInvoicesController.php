<?php

namespace App\Http\Controllers\Api\Members;

use Validator;
use Illuminate\Http\Request;
use Illuminate\Http\Response;

use App\Http\Requests;
use App\Http\Requests\ProformaListRequest;
use App\Http\Controllers\Controller;

use App\Helpers\CoreDB;
use App\Helpers\CoreApi;

use App\Models\Core\Customer;
use App\Helpers\Lib\Api\ApiException;

use Exception;
use Log;
use DB;
use Cache;
use PHP_Timer;

class ProformaInvoicesController extends Controller
{

    public function getList(ProformaListRequest $request)
    {
        $response = [];
        $status_code = 400;
        
        try {

            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemesIncludingMembership($customer_id)->pluck('id');

            $query = $this->getProformaInvoicesQuery()
                ->whereIn('account_charges.customers_scheme_id', $allowed_customers_scheme_ids);
            
            if ($request->input('account_charge_id')) {
                $query->where('account_invoices.account_charge_id', $request->input('account_charge_id'));
            }

            if ($request->input('account_payment_type')) {
                $query->where('account_charges.account_payment_type_id', $request->input('account_payment_type'));
            }

            if ($request->input('invoice_status')) {
                switch ($request->input('invoice_status')) {
                    case 'payment_successful':
                        $query->whereNotNull('account_invoices.id');
                        break;
                    
                    case 'payment_bounced':
                        $query->where('account_charges.bounced', true);
                        break;

                    case 'receipted_invoice_pending':
                        $query->where('account_charges.bounced', false)
                            ->whereNull('account_invoices.id');
                        break;
                }
            }

            if ($request->input('due_date')) {
                $query->where('account_charges.due_date', $request->input('due_date'));
            }

            $total_proforma_invoices = $query->get();
            $total_records = count($total_proforma_invoices);

            $offset = $request->input('offset') ?? 0;
            $limit = $request->input('limit') ?? null;

            $proforma_invoices = $total_proforma_invoices->slice($offset, $limit);

            if (empty($proforma_invoices)) {
                $status_code = 404;
                throw new ApiException('Unable to find proforma invoices.');    
            }

            $response['total_records'] = $total_records;
            $response['returned_records'] = count($proforma_invoices);
            $response['limit'] = $limit;
            $response['results'] = [];
            foreach ($proforma_invoices as $proforma_invoice) {
                $response['results'][] = CoreDB::toCakeArray($proforma_invoice);
            }
            $status_code = 200;

        } catch (ApiException $e) {
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

    public function getDocument(Request $request, $id)
    {
        $response = [];
        $status_code = 400;
        
        try {
            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemesIncludingMembership($customer_id)->pluck('id');
            $allowed_proforma_invoice = $this->getProformaInvoicesQuery()
                ->whereIn('account_charges.customers_scheme_id', $allowed_customers_scheme_ids)
                ->where('account_charges.id', $id)
                ->first();
            
            if (!$allowed_proforma_invoice) {
                throw new ApiException('You do not have permission to view this document.');
            }
            
            $core_api = CoreApi::queryApi([
                'url' => '/InvoiceApi/getDocument.json',
                'parameters' => [
                    'id'   => $id,
                    'type' => 'proforma_invoice',
                ],
            ]);
            $response = $core_api['response'];
            $response_status = $response['response_status'] ?? 400;

            if ($response_status != 200) {
                $status_code = $response_status;
                throw new Exception('Unable to query Core API: ' . $response['error']);
            }

            $status_code = 200;

        } catch (ApiException $e) {
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

    public function getInfo(Request $request, $id)
    {
        $response = [];
        $status_code = 400;
        
        try {
            $customer_id = $request->get('customer_id');
            $allowed_customers_scheme_ids = Customer::getAllowedCustomersSchemesIncludingMembership($customer_id)->pluck('id');
            $allowed_proforma_invoice = $this->getProformaInvoicesQuery()
                ->whereIn('account_charges.customers_scheme_id', $allowed_customers_scheme_ids)
                ->where('account_charges.id', $id)
                ->first();
                        
            if (!$allowed_proforma_invoice) {
                throw new ApiException('You do not have permission to view this document.');
            }
            
            if (empty($allowed_proforma_invoice)) {
                $status_code = 404;
                throw new ApiException('Unable to find proforma invoices.');    
            }

            $response = CoreDB::toCakeArray($allowed_proforma_invoice);

            $account_items = $this->getAccountItemsQuery($id)->get();
            $response['AccountItems'] = [];
            foreach ($account_items as $account_item) {
                $response['AccountItems'][] = CoreDB::toCakeArray($account_item);
            }

            $status_code = 200;

        } catch (ApiException $e) {
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
    
    private function getProformaInvoicesQuery()
    {
        return DB::connection('coredb')
            ->table('account_charges')
            ->leftJoin('account_payment_types', 'account_charges.account_payment_type_id', '=', 'account_payment_types.id')
            ->leftJoin('account_invoices', 'account_charges.id', '=', 'account_invoices.account_charge_id')
            ->where('account_charges.void', 0)
            ->groupBy('account_charges.id')
            ->select(array_merge(
                [DB::raw('(SELECT sum(round((AccountItem.amount) + ( (AccountItem.amount / 100) * (SELECT value FROM taxes AS Tax WHERE Tax.id = AccountItem.tax_id)), 2)) FROM account_items AS AccountItem WHERE AccountItem.account_charge_id = account_charges.id AND AccountItem.void = 0 AND AccountItem.refunded = 0 AND AccountItem.cancelled = 0) as total_inc_tax')],
                CoreDB::prefixColumns([
                            'account_charges' => ['fields' => ['id', 'account_charge_type_id', 'amount', 'customers_scheme_id', 'bank_reference', 'account_payment_type_id', 'bounced', 'invoice_delay', 'auto_invoice', 'due_date', 'dosh_ref', 'void', 'created']],
                            'account_payment_types' => ['fields' => []],
                            'account_invoices' => ['fields' => []],
                        ])));
    }

    private function getAccountItemsQuery($account_charge_id)
    {
        return DB::connection('coredb')
            ->table('account_items')
            ->leftJoin('jobs', 'account_items.relation_id', '=', 'jobs.id')
            ->leftJoin('account_fee_types', 'account_fee_types.id', '=', 'account_items.account_fee_type_id')
            ->leftJoin('policy_types', 'policy_types.id', '=', 'account_items.policy_type_id')
            ->leftJoin('policies', 'policies.account_item_id', '=', 'account_items.id')
            ->leftJoin('schemes', 'schemes.id', '=', 'policies.scheme_id')
            ->where('account_items.void', 0)
            ->where('account_items.account_charge_id', $account_charge_id)
//->where('account_items.account_relation_type_id', 'jobs_contractor')
            ->select(array_merge(
                [DB::raw('(SELECT GROUP_CONCAT(WorkType.short_name) FROM jobs_work_types JobsWorkType INNER JOIN work_types WorkType ON WorkType.id = JobsWorkType.work_type_id WHERE JobsWorkType.job_id = jobs.id) as work_types')],
                [DB::raw('ROUND(account_items.amount + ( (account_items.amount / 100) * (SELECT value FROM taxes WHERE taxes.id = account_items.tax_id) ), 2) as amount_inc_tax')],
                CoreDB::prefixColumns([
                'account_items' => ['fields' => ['account_relation_type_id', 'amount', 'amount_inc_tax', 'refunded', 'cancelled']],
                'jobs' => ['fields' => ['id', 'last_name_or_property_owner', 'address_1', 'address_postcode', 'install_address_differs', 'install_address_1', 
                    'install_address_postcode', 'contract_value', 'completion', 'reference'
                ]],
                'account_fee_types' => ['fields' => ['name']],
                'policies' => ['fields' => ['policy_id']],
                'schemes' => ['fields' => ['policy_prefix']],
            ])));
    }

}