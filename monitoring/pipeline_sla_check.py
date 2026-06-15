def check_pipeline_sla(runtime_minutes: int):
    if runtime_minutes > 30:
        raise Exception("SLA Breach: pipeline runtime exceeded 30 minutes")

    return True
