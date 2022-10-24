import shortuuid

def check_skip_config(**kwargs):
    should_skip_etl = kwargs['dag_run'].conf.get('skip_etl') == "true"
    print(f"Skip ETL? {should_skip_etl}")
    if should_skip_etl:
        return "get_model_digest"
    return "assign_dataset_id"

def assign_dataset_id():
    dataset_id = str(shortuuid.uuid())
    print(f"Assigning dataset_id: {dataset_id}")
    return str(dataset_id)