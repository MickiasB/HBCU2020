# /usr/bin/env python3
import json
import os
import pathlib

import click
import requests
from cpeparser import CpeParser

CURRENT_PATH = os.path.dirname(os.getcwd())


def load_epss():
    f = open(f"{pathlib.Path(__file__).parent.resolve()}/epss.json")
    return json.load(f)


def to_file(file_path, output):
    with open(file_path, "w") as outfile:
        outfile.write(output)


def load_vulnerabilities():

    f = open(f"{pathlib.Path(__file__).parent.resolve()}/vulnerabilities.json")
    return json.load(f)


def products_found(product_cpes, cpe: CpeParser):
    products_found = []
    for cpe_str in product_cpes:
        result = cpe.parser(cpe_str)
        prod = result['product']
        if prod not in products_found:
            products_found.append(prod)
    return products_found


@click.command()
def collect_vulnerabilities():
    """ Get vulnerabilites for each epss CVE in epss.json """
    # 100 CVE scores from https://api.first.org/data/v1/epss (preloaded from local file)
    # Find each CVE details from https://cve.circl.lu/api/cve
    
    print("Loading EPSS data...")
    epss_data = load_epss()['data']

    vulns = []
    cpe_parser = CpeParser()

    print("Collecting CVE metadata and product names for every cve id....")
    for epss in epss_data:
        cve_response = requests.get(f"https://cve.circl.lu/api/cve/{epss['cve']}").json()
        product_cpes = cve_response['vulnerable_product']
        products = products_found(product_cpes, cpe_parser)
        if products:
            vulns.append({
                "product": products,
                "cve_id": cve_response['id'],
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
            })
            print(f"Collected data for CVE_ID: {cve_response['id']}")

    to_file(f"vulnerabilities.json", json.dumps(vulns, indent=4))
