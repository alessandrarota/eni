from api_service import *
from constants import *

def main():
    policy_setup = [
        (create_policy_engine, f"{POLICY_ENGINE_PATH}opa_policy_engine.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP001_valori_ammessi_per_business_domain.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP002_formato_mail_per_owner_id.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP003_valorizzazione_di_owner_name.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP004_valorizzazione_di_description.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP005_valorizzazione_di_name.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP006_valorizzazione_di_version.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP007_valorizzazione_di_contact_point.json"),
        (create_policy, f"{GENERAL_DISCOVERABILITY_POLICY_PATH}GGP008_valori_ammessi_per_tags.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP009_valorizzazione_di_output_port.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP010_valori_ammessi_per_output_port_services_type.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP011_valori_ammessi_per_output_port_specification.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP012_relazione_tra_output_port_services_type_e_specification.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP013_valorizzazione_di_output_port_description.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP014_valorizzazione_di_output_port_version.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP015_valorizzazione_di_output_port_datastoreapi_table_description.json"),
        (create_policy, f"{DATA_CONTRACT_POLICY_PATH}GGP016_valorizzazione_di_input_port.json")
    ]
    for func, payload in policy_setup:
        func(payload)

if __name__ == "__main__":
    main()
