import boto3
import uuid
import json
import os

import botocore
from botocore import exceptions

import config_settings
from creds import ACCESS_KEY, SECRET_KEY

emr_client = boto3.client('emr',
                          region_name='us-east-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

s3_client = boto3.client('s3',
                         region_name='us-east-1',
                         aws_access_key_id=ACCESS_KEY,
                         aws_secret_access_key=SECRET_KEY)

ssm_client = boto3.client('ssm',
                          region_name='us-east-1',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)

logs_client = boto3.client('logs',
                           region_name='us-east-1',
                           aws_access_key_id=ACCESS_KEY,
                           aws_secret_access_key=SECRET_KEY)

s3_resource = boto3.resource('s3',
                             region_name='us-east-1',
                             aws_access_key_id=ACCESS_KEY,
                             aws_secret_access_key=SECRET_KEY)


def upload_s3(jar_file, data_file, folder, data_is_folder, custom_id):
    cwd = os.getcwd()
    changed = dict({"new_jar_name": "", "new_data_name": "", "uploaded_jar": False, "uploaded_data": False})
    if jar_file != "":
        jar_file = cwd + f'/MapReduce/{folder}/' + jar_file
        jar_name = jar_file.split("/")[-1]
        print(f'Uploading {jar_file} to s3...')
        try:
            s3_resource.Bucket('map-reduce-dist-system').Object(f'lambda-emr/jars/{jar_name}').load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print(f'Uploading {jar_file} to map-reduce-dist-system/lambda-emr/jars as {jar_name}...')
                s3_client.upload_file(jar_file, 'map-reduce-dist-system', f'lambda-emr/jars/{jar_name}')
                print(f'Upload of {jar_file} complete!')
                changed['uploaded_jar'] = True
            else:
                print('Something has gone wrong with accessing s3 bucket. Exiting...')
                exit(1)
        else:
            jar_name_split = jar_name.rsplit('.', 1)
            new_jar_name = jar_name_split[0] + '_' + str(custom_id) + '.' + jar_name_split[1]
            print(f'Uploading {jar_name} to map-reduce-dist-system/lambda-emr/jars as {new_jar_name}...')
            s3_client.upload_file(jar_file, 'map-reduce-dist-system', f'lambda-emr/jars/{new_jar_name}')
            print(f'Upload of {jar_file} complete!')
            changed['new_jar_name'] = new_jar_name
    if data_file != "":
        data_file = cwd + f'/MapReduce/{folder}/' + data_file
        data_name = data_file.split("/")[-1]
        print(f'Uploading {data_file} to s3...')
        if data_is_folder == "True":
            resp = s3_client.list_objects(Bucket='map-reduce-dist-system', Prefix=f'lambda-emr/data/{data_name}',
                                          Delimiter='/', MaxKeys=1)
            new_data_folder_name = data_name
            if 'CommonPrefixes' in resp:
                new_data_folder_name = data_name + '_' + str(custom_id)
                print(f'Uploading data to map-reduce-dist-system/lambda-emr/data/{new_data_folder_name}...')
                for subdir, dirs, files in os.walk(f"{cwd}/MapReduce/{folder}/{data_name}"):
                    if len(files) < 1:
                        print('No data files in the specified folder, please add some files and try again')
                        return changed
                    for file in files:
                        if file.startswith('.'):
                            continue
                        file_path = os.path.join(subdir, file)
                        subdirectories = file_path.replace(f"{cwd}/MapReduce/{folder}/{data_name}/", "")
                        subdirectories = subdirectories.replace(f'{file}', "")
                        if subdirectories != "":
                            print(
                                f'Uploading {file_path} to map-reduce-dist-system/lambda-emr/data/{new_data_folder_name}/{subdirectories}...')
                            s3_client.upload_file(file_path, 'map-reduce-dist-system',
                                                  f'lambda-emr/data/{new_data_folder_name}/{subdirectories}{file}')
                        else:
                            print(
                                f'Uploading {file_path} to map-reduce-dist-system/lambda-emr/data/{new_data_folder_name}...')
                            s3_client.upload_file(file_path, 'map-reduce-dist-system',
                                                  f'lambda-emr/data/{new_data_folder_name}/{file}')
                        print(f'Upload of {file_path} complete!')
                print(f'Upload of data in {data_file} complete!')
                changed['new_data_name'] = new_data_folder_name
            else:
                print(f'Uploading data to map-reduce-dist-system/lambda-emr/data/{new_data_folder_name}...')
                for subdir, dirs, files in os.walk(f"{cwd}/MapReduce/{folder}/{data_name}"):
                    if len(files) < 1:
                        print('No data files in the specified folder, please add some files and try again')
                        return changed
                    for file in files:
                        if file.startswith('.'):
                            continue
                        file_path = os.path.join(subdir, file)
                        subdirectories = file_path.replace(f"{cwd}/MapReduce/{folder}/{data_name}/", "")
                        subdirectories = subdirectories.replace(f'{file}', "")
                        if subdirectories != "":
                            print(
                                f'Uploading {file_path} to map-reduce-dist-system/lambda-emr/data/{data_name}/{subdirectories}...')
                            s3_client.upload_file(file_path, 'map-reduce-dist-system',
                                                  f'lambda-emr/data/{data_name}/{subdirectories}{file}')
                        else:
                            print(f'Uploading {file_path} to map-reduce-dist-system/lambda-emr/data/{data_name}...')
                            s3_client.upload_file(file_path, 'map-reduce-dist-system',
                                                  f'lambda-emr/data/{data_name}/{file}')
                        print(f'Upload of {file_path} complete!')
                print(f'Upload of {data_file} complete!')
                changed['uploaded_data'] = True

        else:
            try:
                s3_resource.Bucket('map-reduce-dist-system').Object(f'lambda-emr/data/{data_name}').load()
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print(f'Uploading {data_name} to map-reduce-dist-system/lambda-emr/data as {data_name}...')
                    s3_client.upload_file(data_file, 'map-reduce-dist-system', f'lambda-emr/data/{data_name}')
                    print(f'Upload of {data_file} complete!')
                    changed['uploaded_data'] = True
                else:
                    print('Something has gone wrong with accessing s3 bucket. Exiting...')
                    exit(1)
            else:
                if '.' in data_name:
                    data_name_split = data_name.rsplit('.', 1)
                    new_data_name = data_name_split[0] + '_' + str(custom_id) + '.' + data_name_split[1]
                    print(f'Uploading {data_name} to map-reduce-dist-system/lambda-emr/data as {new_data_name}...')
                    s3_client.upload_file(data_file, 'map-reduce-dist-system', f'lambda-emr/data/{new_data_name}')
                    print(f'Upload of {data_file} complete!')
                    changed['new_data_name'] = new_data_name
                else:
                    new_data_name = data_name + '_' + str(custom_id)
                    print(f'Uploading {data_name} to map-reduce-dist-system/lambda-emr/data as {new_data_name}...')
                    s3_client.upload_file(data_file, 'map-reduce-dist-system', f'lambda-emr/data/{new_data_name}')
                    print(f'Upload of {data_file} complete!')
                    changed['new_data_name'] = new_data_name
    return changed


def upload_cloudwatch_files(job_flow_id, custom_id):
    with open('AmazonCloudWatch-EMRLogs.json', 'r') as cloud_watch_file:
        data = json.load(cloud_watch_file)
        for i in data['logs']['logs_collected']['files']['collect_list']:
            i['log_group_name'] = i['log_group_name'].replace('{JOB_FLOW_ID}', job_flow_id)
        ssm_client.put_parameter(
            Name=f'AmazonCloudWatch-EMRLogs-{job_flow_id}',
            Description=f'CloudWatch Logs Config file for job EMR cluster {job_flow_id}',
            Value=json.dumps(data, indent=4),
            Type='String',
            Tier='Standard',
            DataType='text'
        )
    with open('bootstrap_cloudwatch_agent.sh', 'r') as bootstrap_agent:
        file_data = bootstrap_agent.read()
        file_data = file_data.replace('{JOB_FLOW_ID}', job_flow_id)
        s3_client.put_object(
            Bucket='map-reduce-dist-system',
            Key=f'lambda-emr/bootstrap/{custom_id}_cloudwatch_agent.sh',
            Body=file_data
        )


def run_emr(steps_list, custom_id, instance_type):
    response = emr_client.run_job_flow(
        Name=f'hadoop_job_{custom_id}_cluster',
        LogUri='s3://map-reduce-dist-system/logs',
        ReleaseLabel='emr-6.13.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': instance_type,
                    'InstanceCount': 1
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': instance_type,
                    'InstanceCount': 4
                }
            ],
            'Ec2KeyName': 'EMR',
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-048e9e57bf9e9e330',
            'EmrManagedMasterSecurityGroup': 'sg-01326a6ae6e26935c',
            'EmrManagedSlaveSecurityGroup': 'sg-03f60a5a7f7c25cb7',
        },
        Applications=[{'Name': 'Hadoop'}],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        Steps=steps_list,
        StepConcurrencyLevel=4,
        Configurations=[
            {
               "Classification": "yarn-site",
               "Properties": {
                   "yarn.node-labels.enabled": "true",
                   "yarn.node-labels.am.default-node-label-expression": 'CORE',
                   "yarn.node-labels.fs-store.root-dir": '/apps/yarn/nodelabels',
                   "yarn.node-labels.configuration-type": 'distributed',
                   "yarn.scheduler.capacity.root.support.user-limit-factor": '2',
                   "yarn.nodemanager.disk-health-checker.min-healthy-disks": '0.0',
                   "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage": '100.0'
               }
            },
            {
               "Classification": "capacity-scheduler",
               "Properties": {
                   "yarn.scheduler.capacity.root.accessible-node-labels": '*',
                   "yarn.scheduler.capacity.root.accessible-node-labels.CORE.capacity": "100",
                   "yarn.scheduler.capacity.root.default.accessible-node-labels": '*',
                   "yarn.scheduler.capacity.root.default.accessible-node-labels.CORE.capacity": "100"
               }
            }
        ] + config_settings.configurations[instance_type],
        BootstrapActions=[
            {
                'Name': 'Setup Yarn',
                'ScriptBootstrapAction': {
                    'Path': f's3://map-reduce-dist-system/lambda-emr/bootstrap/wait_for_hadoop_install.sh',
                }
            },
            {
                'Name': 'Install CloudWatch',
                'ScriptBootstrapAction': {
                    'Path': f's3://map-reduce-dist-system/lambda-emr/bootstrap/{custom_id}_cloudwatch_agent.sh',
                }
            }
        ],
        AutoTerminationPolicy={
            'IdleTimeout': 1200
        },
    )
    return response


'''
'''


def generate_steps(job_list, custom_id):
    steps_list = list()
    for i in job_list:
        current_dict = dict(i)
        data_name = current_dict['data_value'].split('/')[-1]
        if current_dict['jar_is_hadoop_local'] == "True":
            jar_name = current_dict['jar_value']
            steps_list.append(
                {
                    'Name': f"{current_dict['job_name']}-{str(custom_id)}",
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            'hadoop',
                            'jar',
                            f'{jar_name}',
                            'wordcount',
                            f's3://map-reduce-dist-system/lambda-emr/data/{data_name}',
                            's3://map-reduce-dist-system/lambda-emr/outputs/report_'
                            f"{current_dict['job_name']}_{str(custom_id).replace('-', '_')}/"
                        ]
                    }
                }
            )
        else:
            jar_name = current_dict['jar_value'].split('/')[-1]
            jar_s3_path = f"s3://map-reduce-dist-system/lambda-emr/jars/{jar_name}"
            steps_list.append(
                {
                    'Name': f"{current_dict['job_name']}-{str(custom_id)}",
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': f'{jar_s3_path}',
                        'MainClass': f"{current_dict['class_name']}",
                        'Args': [
                            f's3://map-reduce-dist-system/lambda-emr/data/{data_name}',
                            's3://map-reduce-dist-system/lambda-emr/outputs/report_'
                            f"{current_dict['job_name']}_{str(custom_id).replace('-', '_')}/"
                        ]
                    }
                }
            )

    return steps_list


def evaluate_needed_uploads(new_job, custom_id):
    for i in new_job['data']:
        jar_file_path = ""
        data_file_path = ""
        old_jar = ""
        old_data = ""
        cwd = os.getcwd()
        if i['jar_already_on_s3'] == 'False' and i['jar_is_hadoop_local'] == 'False':
            jar_file_path = i['jar_value']
        if i['data_already_on_s3'] == 'False':
            data_file_path = i['data_value']
        if jar_file_path != "" or data_file_path != "":
            names_changed = upload_s3(jar_file_path, data_file_path, i['folder'], i['data_is_folder'], custom_id)
            if names_changed != dict({"new_jar_name": "", "new_data_name": ""}):
                if names_changed['new_jar_name'] != "":
                    print(f"Renaming {i['jar_value']} to {names_changed['new_jar_name']}...")
                    print('Changing value data_already_on_s3 to True...')
                    old_jar = f"{cwd}/MapReduce/{i['folder']}/" + i['jar_value']
                    i['jar_value'] = 'jar/' + names_changed['new_jar_name']
                    i['jar_already_on_s3'] = "True"
                if names_changed['new_data_name'] != "":
                    print(f"Renaming {i['data_value']} to {names_changed['new_data_name']}...")
                    print('Changing value data_already_on_s3 to True...')
                    old_data = f"{cwd}/MapReduce/{i['folder']}/" + i['data_value']
                    if i['data_is_folder'] != "True":
                        i['data_value'] = 'data/' + names_changed['new_data_name']
                    else:
                        i['data_value'] = names_changed['new_data_name']
                    i['data_already_on_s3'] = 'True'
                if names_changed['uploaded_jar']:
                    print('Changing value jar_already_on_s3 to True...')
                    i['jar_already_on_s3'] = "True"
                if names_changed['uploaded_data']:
                    print('Changing value data_already_on_s3 to True...')
                    i['data_already_on_s3'] = 'True'
                change_job_conf(old_jar, old_data, i['folder'], without_keys(i, {'folder', 'data_is_folder'}))
                print("Finished changing files!")
    return new_job


def get_jobs():
    cwd = os.getcwd()
    jobs = dict({'data': []})
    job_folders = os.listdir(f'{cwd}/MapReduce')
    for i in job_folders:
        if os.path.isdir(f'{cwd}/MapReduce/{i}') and 'job.conf' in os.listdir(f'{cwd}/MapReduce/{i}'):
            with open(f'{cwd}/MapReduce/{i}/job.conf', 'r') as job_file:
                job_info = json.load(job_file)
                job_info['folder'] = i
                if os.path.exists(f"{cwd}/MapReduce/{i}/{job_info['data_value']}") or job_info[
                    'data_already_on_s3'] == 'True':
                    if os.path.exists(f"{cwd}/MapReduce/{i}/{job_info['jar_value']}") or (
                            job_info['jar_already_on_s3'] == 'True' or job_info['jar_is_hadoop_local'] == 'True'):
                        if os.path.isdir(f"{cwd}/MapReduce/{i}/{job_info['data_value']}"):
                            job_info['data_is_folder'] = "True"
                        else:
                            job_info['data_is_folder'] = "False"
                        jobs['data'].append(job_info)
    return jobs


def change_job_conf(rename_jar, rename_data, folder, dictionary):
    cwd = os.getcwd()
    with open(f'{cwd}/MapReduce/{folder}/job.conf', 'w') as job_file:
        job_file.write(json.dumps(dictionary, indent=4))
    if rename_jar != "":
        os.rename(rename_jar, f"{cwd}/MapReduce/{folder}/{dictionary['jar_value']}")
    if rename_data != "":
        os.rename(rename_data, f"{cwd}/MapReduce/{folder}/{dictionary['data_value']}")


def without_keys(d, keys):
    return {x: d[x] for x in d if x not in keys}


def main():
    custom_id = uuid.uuid4()

    print('Please select the job you want to run from the list:\n')

    job_data = get_jobs()
    new_job = dict({'data': []})
    continue_loop = True

    while continue_loop:
        continue_loop = False
        for i in range(len(job_data['data'])):
            print(f"{i + 1}. {dict(list(job_data['data'])[i])['job_name']}")

        user_in = input(f"{len(job_data['data']) + 1}."
                        "New Job\n\nYou can select a single job or multiple separated by a "
                        f"comma (i.e. 1,2,3)\nJob selection: ")

        user_in = user_in.split(',')
        for i in user_in:
            try:
                new_job['data'].append(dict(list(job_data['data'])[int(i) - 1]))
            except:
                print("Incorrect input, please only type numbers separated by commas with no spaces.\n")
                continue_loop = True
                break

    new_job = evaluate_needed_uploads(new_job, custom_id)

    steps_info = []
    for i in range(6):
        steps_info += generate_steps(list(new_job['data']), f'{custom_id}_{i}')

    emr_data = run_emr(steps_info, custom_id, 'm5.xlarge')
    print(emr_data)
    upload_cloudwatch_files(emr_data['JobFlowId'], custom_id)


def test():
    info = [
               {
                   "Classification": "yarn-site",
                   "Properties": {
                       "yarn.nodemanager.node-labels.provider": 'config',
                       "yarn.nodemanager.node-labels.provider.configured-node-partition": 'CORE'
                   }
               }
           ] + config_settings.configurations['m1.medium']
    print(info)


if __name__ == "__main__":
    main()
