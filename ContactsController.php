<?php

namespace App\Http\Controllers\Api\Members;

use Validator;
use Illuminate\Http\Request;
use Illuminate\Http\Response;

use App\Http\Requests;
use App\Http\Requests\ContactsRequest;
use App\Http\Requests\ContactsEditRequest;
use App\Http\Controllers\Controller;
use App\Models\Core\Customer;
use App\Models\Core\CustomersContact;

use App\Helpers\CoreDB;
use App\Helpers\CoreApi;

use Exception;
use App\Helpers\Lib\Api\ApiException;

use Carbon\Carbon;
use Log;
use DB;

class ContactsController extends Controller
{

    public function add(ContactsRequest $request)
    {
        $response = [];
        $status_code = 400;

        try {
            $customer_id = $request->get('customer_id');
            $response = [];

            $contact_id = DB::connection('coredb')->table('customers_contacts')->insertGetId([
                'customer_id' => $customer_id,
                'type' => $request->input('type'),
                'name' => $request->input('name'),
                'landline' => $request->input('landline'),
                'mobile' => $request->input('mobile'),
                'email' => $request->input('email'),
                'time_to_call' => $request->input('time_to_call'),
                'free_text' => $request->input('free_text'),
                'created' => Carbon::now(),
                'modified' => Carbon::now(),
            ]);

            if (!$contact_id) {
                throw new ApiException('Unable to save record');
            }

            $response['success'] = true;
            $response['id'] = $contact_id;

            $status_code = 200;
        } catch (ApiException $e) {
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            if ($status_code == 200) {
                $status_code = 500;
            }
            Log::error($e->getMessage());
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function edit(ContactsEditRequest $request, $id)
    {
        $response = [];
        $status_code = 400;

        try {
            $customer_id = $request->get('customer_id');
            $response = [];

            $validator = Validator::make([
                'id' => $id,
            ], [
                'id' => 'required|integer',
            ]);

            if ($validator->fails()) {
                throw new ApiException('ID is required');
            }

            $saved = DB::connection('coredb')
                ->table('customers_contacts')
                ->where('id', $id)
                ->where('customer_id', $customer_id)
                ->update([
                    'type' => $request->input('type'),
                    'name' => $request->input('name'),
                    'landline' => $request->input('landline'),
                    'mobile' => $request->input('mobile'),
                    'email' => $request->input('email'),
                    'time_to_call' => $request->input('time_to_call'),
                    'free_text' => $request->input('free_text'),
                    'created' => Carbon::now(),
                    'modified' => Carbon::now(),
                ]);

            if (!$saved) {
                throw new ApiException('Unable to edit record');
            }

            $response['success'] = true;
            $response['id'] = $request->input('id');

            $status_code = 200;
        } catch (ApiException $e) {
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            if ($status_code == 200) {
                $status_code = 500;
            }
            Log::error($e->getMessage());
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function getList(Request $request)
    {
        $response = [];
        $status_code = 400;

        try {
            $customer_id = $request->get('customer_id');
            $response = [];


            $contacts = DB::connection('coredb')
                ->table('customers_contacts')
                ->where('customer_id', $customer_id)
                ->get();

            $response['contacts'] = $contacts;

            $status_code = 200;
        } catch (ApiException $e) {
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            if ($status_code == 200) {
                $status_code = 500;
            }
            Log::error($e->getMessage());
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

    public function getDetails(Request $request, $id)
    {
        $response = [];
        $status_code = 400;

        try {
            $customer_id = $request->get('customer_id');
            $response = [];

            $validator = Validator::make([
                'id' => $id,
            ], [
                'id' => 'required|integer',
            ]);

            if ($validator->fails()) {
                throw new ApiException('ID is required');
            }

            $contacts = DB::connection('coredb')
                ->table('customers_contacts')
                ->where('customer_id', $customer_id)
                ->where('id', $id)
                ->get();

            $response['contacts'] = $contacts;

            $status_code = 200;
        } catch (ApiException $e) {
            $response = ['error' => $e->getMessage()];
            $status_code = $e->getCode() ?: 400;
        } catch (Exception $e) {
            if ($status_code == 200) {
                $status_code = 500;
            }
            Log::error($e->getMessage());
            $response = ['error' => 'Something went wrong'];
        } finally {
            return response()->json($response, $status_code);
        }
    }

}