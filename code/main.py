import json
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import sys
from google.cloud import storage
from google.cloud import bigquery



def checkerIfTagsRequired(resource_tags,user_tags):
    if(entire_labels_comparison(resource_tags,user_tags)!=True):
        return getRequiredLabels(resource_tags,user_tags)
    
    else:
        return None
        
    

def entire_labels_comparison(resource_tags,user_tags):
    if(resource_tags==user_tags):
        return True
    else:
        return False


def getRequiredLabels(resource_tags,user_tags):
    added, removed, modified, same = dict_compare(resource_tags, user_tags)
    final_tags=merge_two_dicts(same,modified)
    final_tags=merge_two_dicts(final_tags,removed)
    
    final_tags_with_keys_values={}

    for key, value in user_tags.items(): 
        if key in final_tags:
            final_tags_with_keys_values[key]=value
            
    return final_tags_with_keys_values

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    shared_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys- d1_keys
    modified = {o for o in shared_keys if d1[o] != d2[o]}
    same = set(o for o in shared_keys if d1[o] == d2[o])
    return added, removed, modified, same



def merge_two_dicts(x, y):
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z
    
    
def tag_bucket_labels(bucket_name,user_labels,user_name):
    """Add a label to a bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket_name)
    bucket_labels = bucket.labels
    
    if(entire_labels_comparison(bucket_labels,user_labels)!=True):

        labels_to_tag=getRequiredLabels(bucket_labels,user_labels)

        strip_creator_name=user_name.replace('@regeneron.com','')
        strip_creator_name_dots=strip_creator_name.replace('.','_')
        labels_to_tag['user_id']=strip_creator_name_dots
        labels_to_tag['autotag']='true'


        bucket.labels = labels_to_tag
        bucket.patch()
    else:
        print("all labels are correct")



def tag_bigQuery_Dataset(project_id_dataset_id,user_labels):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set dataset_id to the ID of the dataset to fetch.
     # dataset_id = "your-project.your_dataset"

    dataset = client.get_dataset(project_id_dataset_id)  # Make an API request.
    dataset.labels = user_labels
    dataset = client.update_dataset(dataset, ["labels"])  # Make an API request.

            

def tag_disk(payload_data,user_tags):

    set_disk_id=payload_data['resource']['labels']['disk_id']
    set_zone=payload_data['resource']['labels']['zone']


    get_project_id=payload_data['resource']['labels']['project_id']
            
    # Project ID for this request.
    project = get_project_id  # TODO: Update placeholder value.

    # The name of the zone for this request.
    zone = set_zone  # TODO: Update placeholder value.
    credentials = GoogleCredentials.get_application_default()


    service = discovery.build('compute', 'v1', credentials=credentials,cache_discovery=False)
    # Name of the instance scoping this request.
    disk = set_disk_id  # TODO: Update placeholder value.
    instance_information = service.disks().get(project=project, zone=zone, disk=disk).execute()
    
    try:
        resource_tags =instance_information['labels']
    except KeyError:
        resource_tags = {}
    

    

    if(checkerIfTagsRequired(resource_tags,user_tags)!=None):


        instance_fingerprint = instance_information['labelFingerprint']

        labels_to_tag=getRequiredLabels(resource_tags,user_tags)


        set_creator_name=payload_data['protoPayload']['authenticationInfo']['principalEmail']
        strip_creator_name=set_creator_name.replace('@regeneron.com','')
        strip_creator_name_dots=strip_creator_name.replace('.','_')
        labels_to_tag['user_id']=strip_creator_name_dots
        labels_to_tag['autotag']='true'

                        

        data = {'labels':labels_to_tag,'labelFingerprint':instance_fingerprint}

        payload_value = json.dumps(data) # indent is optional for easy reading

        json_obj = json.loads(payload_value)

        instances_set_labels_request_body =json_obj

        request = service.disks().setLabels(project=project, zone=zone, resource=disk, body=instances_set_labels_request_body)
        response = request.execute()

    else:
                print('No tag changes required')



def tag_image(payload_data,user_tags):

    set_image_id=payload_data['resource']['labels']['image_id']
    


    get_project_id=payload_data['resource']['labels']['project_id']
            
    # Project ID for this request.
    project = get_project_id  # TODO: Update placeholder value.

    credentials = GoogleCredentials.get_application_default()


    service = discovery.build('compute', 'v1', credentials=credentials,cache_discovery=False)
    # Name of the instance scoping this request.
    image = set_image_id  # TODO: Update placeholder value.
    instance_information = service.images().get(project=project, image=image).execute()
    
    try:
        resource_tags =instance_information['labels']
    except KeyError:
        resource_tags = {}
    

    

    if(checkerIfTagsRequired(resource_tags,user_tags)!=None):


        instance_fingerprint = instance_information['labelFingerprint']

        labels_to_tag=getRequiredLabels(resource_tags,user_tags)


        set_creator_name=payload_data['protoPayload']['authenticationInfo']['principalEmail']
        strip_creator_name=set_creator_name.replace('@regeneron.com','')
        strip_creator_name_dots=strip_creator_name.replace('.','_')
        labels_to_tag['user_id']=strip_creator_name_dots
        labels_to_tag['autotag']='true'

                        

        data = {'labels':labels_to_tag,'labelFingerprint':instance_fingerprint}

        payload_value = json.dumps(data) # indent is optional for easy reading

        json_obj = json.loads(payload_value)

        instances_set_labels_request_body =json_obj

        request = service.images().setLabels(project=project, resource=image, body=instances_set_labels_request_body)
        response = request.execute()

    else:
                print('No tag changes required')




def tag_instance(payload_data,user_tags):

    set_instance_id=payload_data['resource']['labels']['instance_id']
    set_zone=payload_data['resource']['labels']['zone']


    get_project_id=payload_data['resource']['labels']['project_id']
            
    # Project ID for this request.
    project = get_project_id  # TODO: Update placeholder value.

    # The name of the zone for this request.
    zone = set_zone  # TODO: Update placeholder value.
    credentials = GoogleCredentials.get_application_default()


    service = discovery.build('compute', 'v1', credentials=credentials,cache_discovery=False)
    # Name of the instance scoping this request.
    instance = set_instance_id  # TODO: Update placeholder value.
    instance_information = service.instances().get(project=project, zone=zone, instance=instance).execute()
    
    try:
        resource_tags =instance_information['labels']
    except KeyError:
        resource_tags = {}
    

    

    if(checkerIfTagsRequired(resource_tags,user_tags)!=None):


        instance_fingerprint = instance_information['labelFingerprint']

        labels_to_tag=getRequiredLabels(resource_tags,user_tags)


        set_creator_name=payload_data['protoPayload']['authenticationInfo']['principalEmail']
        strip_creator_name=set_creator_name.replace('@regeneron.com','')
        strip_creator_name_dots=strip_creator_name.replace('.','_')
        labels_to_tag['user_id']=strip_creator_name_dots
        labels_to_tag['autotag']='true'

                        

        data = {'labels':labels_to_tag,'labelFingerprint':instance_fingerprint}

        payload_value = json.dumps(data) # indent is optional for easy reading

        json_obj = json.loads(payload_value)

        instances_set_labels_request_body =json_obj

        request = service.instances().setLabels(project=project, zone=zone, instance=instance, body=instances_set_labels_request_body)
        response = request.execute()

    else:
                print('No tag changes required')


def autoTagger(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """
    import base64

    if 'data' in event:
        name = base64.b64decode(event['data']).decode('utf-8')
        final = json.loads(name)
        creator_name=final['protoPayload']['authenticationInfo']['principalEmail']
        resource_type=final['resource']['type']
        project_id=final['resource']['labels']['project_id']
        user_tags= {'sysname':'defaultsystemname', 'environment':'poc', 'department':'isnt_cloud_services','costcenter': '0511','businessunit':'gna','sysowner':'sanjay_sreeram1', 'patchgroup':'cat1', 'avexception':'false','backup': 'false','gxp':'false'}
        


        if(resource_type=='gce_instance'):
            tag_instance(final,user_tags)


            





        elif(resource_type=='gcs_bucket'):
            


            zone=final['resource']['labels']['location']

                        # Project ID for this request.
            project = project_id  # TODO: Update placeholder value.

            # The name of the zone for this request.
            zone = zone  # TODO: Update placeholder value.

            bucket_name=final['resource']['labels']['bucket_name']

            tag_bucket_labels(bucket_name,user_tags,creator_name)


        
  
        

        elif(resource_type=='bigquery_dataset'):
            project=project_id
            dataset_id=final['resource']['labels']['dataset_id']

            final_project_dataset_id=project+"."+dataset_id

            tag_bigQuery_Dataset(final_project_dataset_id,user_tags)
           
                    


        elif(resource_type=='gce_disk'):
            tag_disk(final,user_tags)

        elif(resource_type=='gce_image'):
            tag_image(final,user_tags)



    else:
        print('Function Error')


