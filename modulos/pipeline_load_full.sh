#!/bin/bash

log_file="pipeline_execution.log"
echo "$(date): Starting data pipeline" >> $log_file

# Execute sequential scripts with logging
echo "Executing 01_01-data-generate.py" | tee -a $log_file
if python 01-data-generate.py; then
    echo "$(date): Successfully executed 01-data-generate.py" >> $log_file
    echo "Successfully executed 01-data-generate.py"
else
    echo "$(date): Error executing 01-data-generate.py. Terminating pipeline." >> $log_file
    echo "Error executing 01-data-generate.py. Terminating pipeline."
    exit 1
fi

echo "Executing 02-upload_file_s3.py" | tee -a $log_file
if python 02-upload_file_s3.py; then
    echo "$(date): Successfully executed 02-upload_file_s3.py" >> $log_file
    echo "Successfully executed 02-upload_file_s3.py"
else
    echo "$(date): Error executing 02-upload_file_s3.py. Terminating pipeline." >> $log_file
    echo "Error executing 02-upload_file_s3.py. Terminating pipeline."
    exit 1
fi

echo "Executing 03_observability.py" | tee -a $log_file
if python 03_observability.py; then
    echo "$(date): Successfully executed 03_observability.py" >> $log_file
    echo "Successfully executed 03_observability.py"
else
    echo "$(date): Error executing 03_observability.py. Terminating pipeline." >> $log_file
    echo "Error executing 03_observability.py. Terminating pipeline."
    exit 1
fi

echo "Executing 04_validates_raw_data_quality.py" | tee -a $log_file
if python 04_validates_raw_data_quality.py; then
    echo "$(date): Successfully executed 04_validates_raw_data_quality.py" >> $log_file
    echo "Successfully executed 04_validates_raw_data_quality.py"
else
    echo "$(date): Error executing 04_validates_raw_data_quality.py. Terminating pipeline." >> $log_file
    echo "Error executing 04_validates_raw_data_quality.py. Terminating pipeline."
    exit 1
fi

echo "Executing 05_quality_apply.py" | tee -a $log_file
if python 05_quality_apply.py; then
    echo "$(date): Successfully executed 05_quality_apply.py" >> $log_file
    echo "Successfully executed 05_quality_apply.py"
else
    echo "$(date): Error executing 05_quality_apply.py. Terminating pipeline." >> $log_file
    echo "Error executing 05_quality_apply.py. Terminating pipeline."
    exit 1
fi

echo "Executing 06_validates_clean_data_quality.py" | tee -a $log_file
if python 06_validates_clean_data_quality.py; then
    echo "$(date): Successfully executed 06_validates_clean_data_quality.py" >> $log_file
    echo "Successfully executed 06_validates_clean_data_quality.py"
else
    echo "$(date): Error executing 06_validates_clean_data_quality.py. Terminating pipeline." >> $log_file
    echo "Error executing 06_validates_clean_data_quality.py. Terminating pipeline."
    exit 1
fi

echo "Executing 07_enrichment.py" | tee -a $log_file
if python 07_enrichment.py; then
    echo "$(date): Successfully executed 07_enrichment.py" >> $log_file
    echo "Successfully executed 07_enrichment.py"
else
    echo "$(date): Error executing 07_enrichment.py. Terminating pipeline." >> $log_file
    echo "Error executing 07_enrichment.py. Terminating pipeline."
    exit 1
fi

echo "Executing 08_security.py" | tee -a $log_file
if python 08_security.py; then
    echo "$(date): Successfully executed 08_security.py" >> $log_file
    echo "Successfully executed 08_security.py"
else
    echo "$(date): Error executing 08_security.py. Terminating pipeline." >> $log_file
    echo "Error executing 08_security.py. Terminating pipeline."
    exit 1
fi

echo "$(date): Data pipeline successfully completed." >> $log_file
echo "Data pipeline successfully completed."
