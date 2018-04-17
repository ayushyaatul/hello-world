import pandas as pd
import requests
import json
import psycopg2
import numpy as np
import shapely
from shapely. geometry import MultiPolygon
from shapely.geometry import box
from shapely.geometry import Polygon
from matplotlib import pyplot
def plot_multi_polygon( poly ):
    x,y = poly.exterior.xy
    ax = fig.add_subplot(111)
    ax.plot(x, y, color='#6699cc', alpha=0.7,
    linewidth=3, solid_capstyle='round', zorder=2)
    ax.set_title('Polygon')
def make_connection_prod():
    try:
        conn = psycopg2.connect(
            database='playment_api_production',
            user='ulquiorra865',
            password='a8P$n!perPl@yCoD#',
            host='52.66.188.226',
            port='5432'
        )
        return conn
    except:
        print("Unable to connect to the DB")
        return False
    
def get_project_maker_checker_iterations(project_id):
        conn = make_connection_prod()
        cur = conn.cursor()
        cur.execute("""
                    select Iterations.master_flu_id as master_flu_id, count(Iterations.master_flu_id) as flu_iteration_count
                    from
                            ( select projects.name as project_name, work_flow.id as work_flow_id, work_flow.project_id, work_flow.label as work_flow_label,  
                    step.id as step_id, step_type.name as step_name,fll.master_flu_id as master_flu_id
                    from projects
                    inner join work_flow on work_flow.project_id = projects.id
                    inner join step on step.work_flow_id = work_flow.id
                    inner join step_type on step_type.id = step.type
                    inner join feed_line_log as fll on fll.step_id = step.id
                    inner join crowdsourcing_flu_buffer as cfb on cfb.flu_id = fll.master_flu_id
                    where projects.id  = '%s' and step_type.name = 'COORDINATOR' and fll.event = 1 ) as Iterations
                    Group By Iterations.master_flu_id
                    """ %(project_id))
        project_details = pd.DataFrame(cur.fetchall())
        
        if(len(project_details) == 0):
            project_details = pd.DataFrame()
        else:
            project_details.columns = ['master_flu_id', 'flu_iteration_count']
        return project_details
#input radiobutton -checker
def parse_label_component(label):
    if( label.startswith("input") ):
        component  = label[len("input-"):]
        label_type = "maker"
    elif( label.startswith("checker") ):
        component  = label[len("checker-"):]
        label_type = "checker"
    else:
        component  = "unknown"
        label_type = "unknown"
    return { "component": component, "label_type": label_type } 

def identify_component_from_resources(resources):
    resources = json.loads(resources)
    for resource in resources:
        parsed_resource = parse_label_component(resource['label'])
        if( parsed_resource['label_type'] !=  "unknown" ):
            return parsed_resource['label_type']
    return "unknown"

def identify_label_from_resources(resources):
    resources = json.loads(resources)
    for resource in resources:
        parsed_resource = parse_label_component(resource['label'])
        if( parsed_resource['component'] !=  "unknown" ):
            return parsed_resource['component']
    return "unknown"

resource = json.dumps([{"label":"input-multiple-bounding-box","data":{"image":"{image_url}","correct":"{correct}","incorrect":"{incorrect}","label_type":"MULTI","label_options":["Pedestrian","Person","Motorcycle Rider","Bicycle Rider ","stroller"]},"draw_single":False,"force_quality":"ORIGINAL"},{"label":"header-small","data":"Tips to identify Pedestrian  and make  Pedestrian(other vehicles also) "},{"label":"header-medium","data":"1) Assume the hidden part and make the best fit box"},{"label":"text","data":"<p><br></p>"},{"label":"header-small","data":"2) Use Brightness feature "},{"label":"text","data":"<p><br></p>"},{"label":"header-small","data":"3) Clothes"},{"label":"text","data":"<p><br></p>"},{"label":"header-small","data":"4) Hair"},{"label":"text","data":"<p><br></p>"},{"label":"text","data":"<iframe src=\"https://playmentproduction.s3.amazonaws.com/public/common%20mistakes%202%20wheeler/Slide1.JPG\" height = 400 width = 100%>"}])
x = identify_component_from_resources(resource)
       
def get_workflow_maker_steps(workflow_id, cur):
        cur.execute("""
                    select wf.id as workflow_id, step.id as step_id, mtrc.micro_task_id, r.id as resource_id, r.body as resource_body
                    from work_flow wf
                    inner join step on step.work_flow_id = wf.id
                    inner join step_type on step_type.id = step.type
                    inner join micro_task_resource_associators as mtrc on mtrc.micro_task_id::text = step.config ->> 'micro_task_id'
                    inner join resources as r on mtrc.resource_id = r.id
                    where work_flow_id = '%s' and step_type.name = 'CROWD_SOURCING' 
                          and r.body_type = 'json' and r.label = 'grammar-template'
                    """ %(workflow_id))
        details = pd.DataFrame(cur.fetchall())
                       
        if(len(details) == 0):
            details = pd.DataFrame()
        else:
            details.columns = ['workflow_id', 'step_id', 'micro_task_id', 'resource_id', 'resource_body']
        details['maker_checker_label'] = details['resource_body'].apply(identify_component_from_resources)
        details['task_type'] = details['resource_body'].apply(identify_label_from_resources)
        details = details[ details.maker_checker_label == 'maker']
        return details


def playment_to_shapely_box(annotation):
    return box( annotation[0]['x'], annotation[0]['y'], annotation[1]['x'], annotation[1]['y'] )

def multi_polygon_per_label( maker_results ):
    annotation_by_category =  {};
    multi_polygon_by_category = {};
    for ann in maker_results:
        label = ann['label']
        if label in annotation_by_category:
            annotation_by_category[label].append( playment_to_shapely_box( ann['coordinates'] ) )
        else:
            annotation_by_category[label] = [ playment_to_shapely_box( ann['coordinates'] ) ]
    
    for label,shapes in annotation_by_category.items():
        multi_polygon_by_category[label] = MultiPolygon(shapes)
    return(multi_polygon_by_category)            
            
def precision_multi_polygons( multipolygons ):
    intersec = multipolygons[0].buffer(0)
    union    = multipolygons[0].buffer(0)
    for mp in multipolygons[1:]:
        intersec = intersec.intersection(mp.buffer(0))
        union    = union.union(mp.buffer(0))
        print(intersec.area)
    return(intersec.area/union.area)
    
def get_entropy_per_category(all_maker_flu_annotations):
    step_flu_results = []
    maker_annotation_by_category = {};
    precision_by_category        = {};
    recall_by_category           = {};
    #Merge maker_response and correct_response and get annotations per category
    for index,row in all_maker_flu_annotations.iterrows():
        step_flu_results.append( row['correct_response'] + row['maker_response'] )
        row_result = row['correct_response'] + row['maker_response']
        maker_result = multi_polygon_per_label( row_result )
        for label,multi_polygon in maker_result.items():
            if label in maker_annotation_by_category:
                maker_annotation_by_category[label].append(multi_polygon)
            else:
                maker_annotation_by_category[label] = [multi_polygon] 
    #Precision and recall per category
    
    #Precision is defined as intersection/union of all annotations
    for label,response_array in maker_annotation_by_category.items():
        precision_by_category[label] = precision_multi_polygons(response_array)
    return precision_by_category
    
        
    
 
    
def get_all_flu_annotations(workflow_id):
    precision_entropy_per_flu_id = {}
    conn = make_connection_prod()
    cur = conn.cursor()
    maker_steps = get_workflow_maker_steps(workflow_id, cur)
    
    #create list to query maker steps
    maker_query_tags = '('
    for step_id in list( maker_steps['step_id'] ):
        maker_query_tags = maker_query_tags + '\'' + step_id + '\','
    maker_query_tags = maker_query_tags[0:-1] + ')'       
        
        
    cur.execute("""select master_flu_id, meta_data, step_id, fll.created_at, fll.event,
                meta_data#>'{build, correct}' as correct, meta_data#>'{build,maker_response}' as maker_response       
                from feed_line_log as fll
                where step_id in %s             
                and fll.event = 2
                and master_flu_id = '5fde52fb-6d66-4eb6-abef-91c87b94631e'
                    """ %(maker_query_tags))
    maker_flus = pd.DataFrame(cur.fetchall())
    if(len(maker_flus) == 0):
            maker_flus = pd.DataFrame()
    else:
            maker_flus.columns = [ 'master_flu_id', 'meta_data', 'step_id', 'created_at', 'event_type', 'correct_response', 'maker_response']
    for flu in maker_flus.master_flu_id.unique():
        all_maker_flu_annotations = maker_flus[maker_flus['master_flu_id'] == flu ]
        precision_entropy_per_flu_id[ flu ] = get_entropy_per_category(all_maker_flu_annotations)
    return precision_entropy_per_flu_id




workflow_id = '770e5837-cab5-4604-83e9-5be8b0b28502' 
precision_entropy_per_flu_id = get_all_flu_annotations(workflow_id)


















    
    
