from flask import Flask, request, jsonify
import requests
import json
from datetime import datetime, timedelta
import QuantLib as ql
import pandas as pd
from datetime import datetime


sourceObjLink = "https://access.menaracapital.com/version-test/api/1.1/obj/"
sourceWfLink = "https://access.menaracapital.com/version-test/api/1.1/wf/"

bearer_token = "39e97f5f77e727b2b13269a118fde588"
# Prepare the headers for the request
headers = {
    "Authorization": f"Bearer {bearer_token}"
}

app = Flask(__name__)

def convert_ms_to_date(ms):
    # Convert milliseconds to seconds
    seconds = ms / 1000.0
    # Create a datetime object from the seconds
    date = datetime.utcfromtimestamp(seconds)
    # Format the date as "yyyy-mm-dd 00:00:000"
    formatted_date = date.strftime('%Y-%m-%d 00:00:00')
    return formatted_date

# Function to simulate fetching bond cashflows and outstandings for a given bond
def holdingCcy(bond):
    api_url= sourceObjLink + "bond/" + bond
    # Make the GET request
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        # Successfully received the response
        data = response.json()
        bond_ccy = data["response"]["Ccy"]
    else:
        # Something went wrong
        return jsonify(error="Failed to fetch data"), response.status_code
    return jsonify(bond_ccy)


@app.route('/bond', methods=['GET'])
def get_bond_data():
    # Define the query parameters
    uid = request.args.get('uid', default=0, type=str)
    # Define the API endpoint
    api_url = sourceObjLink + "bond/" + uid
    query_params = {
        'uid': uid,
    }
    # Make the GET request
    response = requests.get(api_url, params=query_params, headers=headers)
    
    if response.status_code == 200:
        # Successfully received the response
        data = response.json()
        return jsonify(data["response"])
    else:
        # Something went wrong
        return jsonify(error="Failed to fetch data"), response.status_code

@app.route('/bondcashflows', methods=['GET'])
def get_bondcashflows_data():
    # Define the query parameters
    uid = request.args.get('uid', default=0, type=str)
    # Define the API endpoint
    api_url = sourceWfLink + "bondcashflows/" + uid
    query_params = {
        'uid': uid,
    }
    print(api_url)
    # Make the GET request
    response = requests.get(api_url, params=query_params, headers=headers)
    
    if response.status_code == 200:
        # Successfully received the response
        data = response.json()
        return jsonify(data["response"])
    else:
        # Something went wrong
        return jsonify(error="Failed to fetch data"), response.status_code

@app.route('/bondtranches', methods=['GET'])
def get_bondtranches_data():
    # Define the query parameters
    uid = request.args.get('uid', default=0, type=str)
    # Define the API endpoint
    api_url = sourceWfLink + "bondtranches/" + uid
    query_params = {
        'uid': uid,
    }
    print(api_url)
    # Make the GET request
    response = requests.get(api_url, params=query_params, headers=headers)
    
    if response.status_code == 200:
        # Successfully received the response
        data = response.json()
        return jsonify(data["response"])
    else:
        # Something went wrong
        return jsonify(error="Failed to fetch data"), response.status_code

@app.route('/bondoutstandings/<bond_id>', methods=['GET'])
def get_bondoutstandings_data(bond_id):
    # Define the query parameters
    uid = bond_id
    # Define the API endpoint
    api_url_tranches = sourceWfLink + "bondtranches/" + uid
    api_url_bondcashflows = sourceWfLink + "bondcashflows/" + uid
    query_params = {
        'uid': uid,
    }
    # Make the GET request
    responsetranches = requests.get(api_url_tranches, params=query_params, headers=headers)
    responsebondcashflows = requests.get(api_url_bondcashflows, params=query_params, headers=headers)
    
    if responsetranches.status_code == 200 & responsebondcashflows.status_code == 200:
        # Successfully received the response
        datatranches = responsetranches.json()
        databondcashflows = responsebondcashflows.json()
        # Convert to pandas DataFrames for easier manipulation
        tranches_df = pd.DataFrame(datatranches["response"]["tranches"])
        cashflows_df = pd.DataFrame(databondcashflows["response"]["list_cashflows"])

        # Convert Unix timestamp to datetime for easier comparison
        tranches_df['Issue Date'] = pd.to_datetime(tranches_df['Issue Date'], unit='ms')
        cashflows_df['Date'] = pd.to_datetime(cashflows_df['Date'], unit='ms')

        # Initialize a list to hold bond outstanding amounts for each cashflow date
        bond_outstandings = []

        # Iterate through each cashflow, calculate the bond outstanding at that date
        for index, cashflow in cashflows_df.iterrows():
            date = cashflow['Date']
            bond_name = cashflow['BondName']
            print(bond_name)
            # Sum the Issue Amounts for tranches before or on the cashflow date
            outstanding_amount = tranches_df[
                (tranches_df['Bond'] == bond_name) & (tranches_df['Issue Date'] <= date)
            ]['Issue Amount'].sum()
            
            bond_outstandings.append({
                "Date": date,
                "BondName": bond_name,
                "BondOutstanding": outstanding_amount
            })
        # Convert the bond outstandings list to a DataFrame
        bond_outstandings_df = pd.DataFrame(bond_outstandings)
        bond_outstandings_json = bond_outstandings_df.to_json(orient='records', date_format='iso')
        # return the result
        return bond_outstandings_json
    else:
        # Something went wrong
        return jsonify(error="Failed to fetch data"), responsetranches.status_code

@app.route('/holdings', methods=['GET'])
def get_holdings_data():
    # Define the query parameters
    pid = request.args.get('pid', default=0, type=str)
    # Define the API endpoint
    api_url = sourceWfLink + "holdings/" + pid
    query_params = {
        'pid': pid,
    }
    print(api_url)
    # Make the GET request
    response = requests.get(api_url, params=query_params, headers=headers)
    
    if response.status_code == 200:
        # Successfully received the response
        data = response.json()
        return jsonify(data["response"])
    else:
        # Something went wrong
        return jsonify(error="Failed to fetch data"), response.status_code

@app.route('/profitandloss', methods=['GET'])
def get_portfoliosummary_data():
    # Define the query parameters
    pid = request.args.get('pid', default=0, type=str)
    # Define the API endpoint
    api_url_holdings = sourceWfLink + "holdings/" + pid
    query_params = {
        'pid': pid,
    }
    # Make the GET request
    responseholdings = requests.get(api_url_holdings, params=query_params, headers=headers)
    if responseholdings.status_code == 200:
        dataholdings = responseholdings.json()
        holdings = dataholdings["response"]["list_holdings"]
        # Placeholder for aggregated results
        results = []

        # Loop through each holding and fetch corresponding bond data
        for holding in holdings:
            uid = holding["Bond"]
            api_url_cashflows = sourceWfLink + "bondcashflows/" + uid
            query_params = {
            'uid': uid,
            }
            # Make the GET request
            responsecashflows = requests.get(api_url_cashflows, params=query_params, headers=headers)
            datacashflows = responsecashflows.json()
            cashflows = datacashflows["response"]["list_cashflows"]

            dataoutstandings = get_bondoutstandings_data(uid)
            outstandings = json.loads(dataoutstandings)

            value_date = convert_ms_to_date(holding["Value Date"])
            i=0
            for cashflow in cashflows:
                cashflow_date = convert_ms_to_date(cashflow["Date"])
                if (cashflow_date > value_date) and (cashflow["Interest"] > 0 or cashflow["Principal"] > 0 or cashflow["Profit Participation"] > 0):
                    portion = holding["Size"] / outstandings[i]["BondOutstanding"]
                    results.append({
                        "Ccy": holdingCcy(uid).json,
                        "Date": cashflow_date,
                        "Interest Received": cashflow["Interest"] * portion,
                        "Principal Received": cashflow["Principal"] * portion,
                        "Profit Participation Received": cashflow["Profit Participation"] * portion
                    })
                i=i+1    
        # Convert results to DataFrame for aggregation
        df_results = pd.DataFrame(results)   
        # Aggregate results by currency
        aggregated = df_results.groupby(['Ccy']).sum().reset_index()

        # Display or use the aggregated data
        return aggregated.to_json(orient='records', date_format='iso')

    else:
        # Something went wrong
        return jsonify(error="Failed to perform calculation")


if __name__ == '__main__':
    app.run(debug=True)
