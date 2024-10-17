# /usr/bin/env python3
import json
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from cpeparser import CpeParser

CURRENT_PATH = os.path.dirname(os.getcwd())


def products_found(product_cpes, cpe: CpeParser):
    products_found = []
    for cpe_str in product_cpes:
        result = cpe.parser(cpe_str)
        prod = result['product']
        if prod not in products_found:
            products_found.append(prod)
    return products_found


def process_epss(epss, endpoint, cpe_parser):
    cve_response = requests.get(f"{endpoint}/{epss['cve']}").json()
    product_cpes = cve_response['vulnerable_product']
    products = products_found(product_cpes, cpe_parser)
    if products:
        return {
            "cve_id": cve_response['id'],
            "productList": products,
            "lastModified": cve_response['last-modified'],
            "publishedDate": cve_response['Published'],
            "assignedby": cve_response.get('assigner', None),
            "summary": cve_response['summary'],
            "vulnerabilityProduct": product_cpes,
            "epss_details": {
                "cve_id": epss['cve'],
                "epss": epss['epss'],
                "percentile": epss['percentile'],
                "date": epss['date']
            }
        }
    return None

class EPSS:
    def __init__(self, config):
        self.header = {
            'Accept': 'application/json',
            'Content-type': 'application/json'
        }
        self.config = config
    
    def load_epss(self):
        """ Load epss data"""

        results = []
        offset = 0
        endpoint = self.config['epssURL'] + "/data/v1/epss"
        querystring = {
            "envelope": True,
            "pretty": True,
            "limit": 30,
            "offset": offset
        }
        try:
            response = requests.post(endpoint, headers=self.header, params=querystring, timeout=20)
        except requests.exceptions.ReadTimeout as e:
            raise ValueError(f"Request to {endpoint} timed out") from e
        if response.status_code != 200:
            raise ValueError(f"Error: received status code {response.status_code}")
        response_json = response.json()
        if not response_json.get('data'):
            print("Error: received empty data object")
            return results
        total_items = response_json.get("total")
        i = 0
        while offset < total_items:
            if i > 5:
                break
            i += 1
            for item in response_json.get("data"):
                item['cve'] = item.get('cve', '')
                item['epss'] = item.get('epss', '0.0') if item.get('epss', '0') != '' else 0.0
                item['percentile'] = item.get('percentile', '0.0') if item.get('percentile', '0') != '' else 0.0
                item['date'] = item.get('date', '')
                results.append(item)
            offset += 30
            querystring["offset"] = offset
            response = requests.post(endpoint, headers=self.header, params=querystring, timeout=20)
            response_json = response.json()
        return results


    def collect_vulnerabilities(self):
        """ Get vulnerabilites for each epss CVE in epss.json """
        
        print("Loading EPSS data...")
        epss_data = self.load_epss()
        
        print(f"Collected {len(epss_data)} EPSS data")

        vulns = []
        cpe_parser = CpeParser()

        print("Collecting CVE metadata and product names for every cve id....")
        endpoint = self.config['cveURL']
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_epss, epss, endpoint, cpe_parser) for epss in epss_data]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    vulns.append(result)
                    print(f"Collected data for CVE_ID: {result['cve_id']}")
    
        return vulns
