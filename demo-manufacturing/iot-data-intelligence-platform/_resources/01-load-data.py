# Databricks notebook source
# MAGIC %pip install git+https://github.com/QuentinAmbard/mandrova faker databricks-sdk==0.17.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %run ../config

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup-v2

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)
folder = f"/Volumes/{catalog}/{db}/{volume_name}"

data_exists = False
try:
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/historical_turbine_status")
  dbutils.fs.ls(folder+"/parts")
  dbutils.fs.ls(folder+"/turbine")
  dbutils.fs.ls(folder+"/incoming_data")
  data_exists = True
  print("data already exists")
except Exception as e:
  print(f"folder doesn't exists, generating the data...")


def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

data_downloaded = True
if not data_exists:
    try:
        DBDemos.download_file_from_git(folder+'/historical_turbine_status', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/historical_turbine_status")
        DBDemos.download_file_from_git(folder+'/parts', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/parts")
        DBDemos.download_file_from_git(folder+'/turbine', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/turbine")
        DBDemos.download_file_from_git(folder+'/incoming_data', "databricks-demos", "dbdemos-dataset", "/manufacturing/lakehouse-iot-turbine/incoming_data")
        spark.sql("CREATE TABLE IF NOT EXISTS turbine_power_prediction ( hour INT, min FLOAT, max FLOAT, prediction FLOAT);")
        spark.sql("insert into turbine_power_prediction values (0, 377, 397, 391), (1, 393, 423, 412), (2, 399, 455, 426), (3, 391, 445, 404), (4, 345, 394, 365), (5, 235, 340, 276), (6, 144, 275, 195), (7, 93, 175, 133), (8, 45, 105, 76), (9, 55, 125, 95), (10, 35, 99, 77), (11, 14, 79, 44)")
    except Exception as e: 
        data_downloaded = False
        print(f"Error trying to download the file from the repo: {str(e)}. Will generate the data instead...")    
        raise e

# COMMAND ----------

#As we need a model in the DLT pipeline and the model depends of the DLT pipeline too, let's build an empty one.
#This wouldn't make sense in a real-world system where we'd have 2 jobs / pipeline (1 for ingestion, and 1 to build the model / run inferences)
import random
import mlflow
from  mlflow.models.signature import ModelSignature

# define a custom model randomly flagging 10% of sensor for the demo init (it'll be replace with proper model on the training part.)
class MaintenanceEmptyModel(mlflow.pyfunc.PythonModel):
    def predict(self, context, model_input):
        import random
        sensors = ['sensor_F', 'sensor_D', 'sensor_B']  # List of sensors
        return model_input['avg_energy'].apply(lambda x: 'ok' if random.random() < 0.9 else random.choice(sensors))
 
#Enable Unity Catalog with mlflow registry
mlflow.set_registry_uri('databricks-uc')
model_name = "dbdemos_turbine_maintenance"
#Only register empty model if model doesn't exist yet
client = mlflow.tracking.MlflowClient()
try:
  latest_model = client.get_model_version_by_alias(f"{catalog}.{db}.{model_name}", "prod")
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print("Model doesn't exist - saving an empty one")
        # setup the experiment folder
        DBDemos.init_experiment_for_batch("lakehouse-iot-platform", "predictive_maintenance_mock")
        # save the model
        churn_model = MaintenanceEmptyModel()
        import pandas as pd

        signature = ModelSignature.from_dict({'inputs': '[{"name": "hourly_timestamp", "type": "datetime"}, {"name": "avg_energy", "type": "double"}, {"name": "std_sensor_A", "type": "double"}, {"name": "std_sensor_B", "type": "double"}, {"name": "std_sensor_C", "type": "double"}, {"name": "std_sensor_D", "type": "double"}, {"name": "std_sensor_E", "type": "double"}, {"name": "std_sensor_F", "type": "double"}, {"name": "percentiles_sensor_A", "type": "string"}, {"name": "percentiles_sensor_B", "type": "string"}, {"name": "percentiles_sensor_C", "type": "string"}, {"name": "percentiles_sensor_D", "type": "string"}, {"name": "percentiles_sensor_E", "type": "string"}, {"name": "percentiles_sensor_F", "type": "string"}, {"name": "location", "type": "string"}, {"name": "model", "type": "string"}, {"name": "state", "type": "string"}]',
'outputs': '[{"type": "tensor", "tensor-spec": {"dtype": "object", "shape": [-1]}}]'})
        with mlflow.start_run() as run:
            model_info = mlflow.pyfunc.log_model(artifact_path="model", python_model=churn_model, signature=signature, pip_requirements=['scikit-learn==1.1.1', 'mlflow==2.4.0'])

        #Register & move the model in production
        model_registered = mlflow.register_model(f'runs:/{run.info.run_id}/model', f"{catalog}.{db}.{model_name}")
        client.set_registered_model_alias(name=f"{catalog}.{db}.{model_name}", alias="prod", version=model_registered.version)
    else:
        print(f"ERROR: couldn't access model for unknown reason - DLT pipeline will likely fail as model isn't available: {e}")

# COMMAND ----------

if data_downloaded:
    dbutils.notebook.exit(f"Data Downloaded to {folder}")

# COMMAND ----------

from mandrova.data_generator import SensorDataGenerator as sdg
import numpy as np
import pandas as pd
import random
import time
import uuid
import pyspark.sql.functions as F


def generate_sensor_data(turbine_id, sensor_conf, faulty = False, sample_size = 1000, display_graph = True, noise = 2, delta = -3):
  dg = sdg()
  rd = random.Random()
  rd.seed(turbine_id)
  dg.seed(turbine_id)
  sigma = sensor_conf['sigma']
  #Faulty, change the sigma with random value
  if faulty:
    sigma *= rd.randint(8,20)/10
    
  dg.generation_input.add_option(sensor_names="normal", distribution="normal", mu=0, sigma = sigma)
  dg.generation_input.add_option(sensor_names="sin", eq=f"2*exp(sin(t))+{delta}", initial={"t":0}, step={"t":sensor_conf['sin_step']})
  dg.generate(sample_size)
  sensor_name = "sensor_"+ sensor_conf['name']
  dg.sum(sensors=["normal", "sin"], save_to=sensor_name)
  max_value = dg.data[sensor_name].max()
  min_value = dg.data[sensor_name].min()
  if faulty:
    n_outliers = int(sample_size*0.15)
    outliers = np.random.uniform(-max_value*rd.randint(2,3), max_value*rd.randint(2,3), n_outliers)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
    dg.inject(value=outliers, sensor=sensor_name, index=indicies)

  n_outliers = int(sample_size*0.01)
  outliers = np.random.uniform(min_value*noise, max_value*noise, n_outliers)
  indicies = np.sort(np.random.randint(0, sample_size-1, n_outliers))
  dg.inject(value=outliers, sensor=sensor_name, index=indicies)
  
  if display_graph:
    dg.plot_data(sensors=[sensor_name])
  return dg.data[sensor_name]

# COMMAND ----------

sensors = [{"name": "A", "sin_step": 0, "sigma": 1},
           {"name": "B", "sin_step": 0, "sigma": 2},
           {"name": "C", "sin_step": 0, "sigma": 3},
           {"name": "D", "sin_step": 0.1, "sigma": 1.5},
           {"name": "E", "sin_step": 0.01, "sigma": 2},
           {"name": "F", "sin_step": 0.2, "sigma": 1}]
current_time = int(time.time()) - 3600*30

#Sec between 2 metrics
frequency_sec = 10
#X points per turbine (1 point per frequency_sec second)
sample_size = 2125
turbine_count = 512
dfs = []

# COMMAND ----------

report_list = [
    """
    Issue: Gearbox failure
    Root Cause: Wear and tear of gearbox components due to prolonged operation under high loads and inadequate lubrication.
    Solution: Replace the gearbox with a new or refurbished unit. Implement a proactive maintenance schedule including regular oil analysis, gearbox inspections, and lubrication system checks to prevent similar failures in the future.
    """,
    """
    Issue: Pitch system malfunction
    Root Cause: Failure of the pitch motor due to electrical faults or mechanical wear, resulting in erratic blade pitch angles and reduced turbine performance.
    Solution: Replace the faulty pitch motor with a new unit and recalibrate the pitch control system. Conduct thorough diagnostic tests to identify and address any underlying electrical or mechanical issues. Implement routine maintenance procedures, including lubrication of moving parts and inspection of electrical connections, to prevent future malfunctions.
    """,
    """
    Issue: Excessive yaw misalignment
    Root Cause: Yaw motor malfunction caused by mechanical wear, electrical faults, or software glitches, leading to misalignment between the turbine rotor and wind direction.
    Solution: Repair or replace the defective yaw motor and realign the turbine according to manufacturer specifications. Perform comprehensive diagnostic tests to ensure proper functioning of the yaw control system. Implement regular maintenance and monitoring procedures to detect and address any emerging issues with the yaw mechanism promptly.
    """,
    """
    Issue: Main bearing overheating
    Root Cause: Inadequate lubrication, bearing misalignment, or contamination of the lubricating oil, resulting in increased friction and heat generation in the main bearing assembly.
    Solution: Conduct a thorough inspection of the main bearing and lubrication system to identify the root cause of overheating. Replace worn-out bearings, realign the bearing housing, and flush and replenish the lubricating oil with a high-quality, manufacturer-approved product. Implement regular oil analysis and bearing temperature monitoring to detect early signs of degradation and prevent catastrophic failures.
    """,
    """
    Issue: Tower vibrations
    Root Cause: Resonance with wind frequencies due to structural instabilities, improper damping, or aerodynamic interactions, leading to oscillations and fatigue stress on tower components.
    Solution: Conduct a detailed structural analysis of the tower to identify resonance frequencies and structural weaknesses. Install vibration dampers, tuned mass dampers, or other damping devices to mitigate vibrations and reduce stress on tower elements. Implement ongoing monitoring and inspection programs to assess the effectiveness of damping measures and ensure the long-term integrity of the tower structure.
    """,
    """
    Issue: Brake system failure
    Root Cause: Hydraulic fluid leakage from the brake system components, such as actuators, valves, or hydraulic lines, resulting in reduced braking force and compromised safety during turbine shutdown.
    Solution: Inspect the entire hydraulic brake system for leaks, worn-out seals, or damaged components. Repair or replace faulty parts and replenish the hydraulic fluid with a compatible, high-performance product. Conduct functional tests and brake performance checks to verify proper operation and safety compliance. Implement regular maintenance procedures, including hydraulic system inspections and fluid level checks, to prevent future brake failures.
    """,
    """
    Issue: Gear tooth damage
    Root Cause: Misalignment, overload, or improper lubrication of gearbox components, leading to excessive wear and fatigue on gear teeth.
    Solution: Realign the gearbox housing and inspect gear teeth for damage or wear patterns. Replace worn-out or damaged gears with high-quality, precision-engineered replacements. Implement a rigorous lubrication maintenance program, including regular oil changes, filtration, and analysis, to ensure proper lubrication and minimize friction-induced wear. Conduct comprehensive load and stress analysis to optimize gearbox performance and prevent future gear tooth failures.
    """,
    """
    Issue: Low rotor speed
    Root Cause: Malfunctioning wind sensor, inaccurate wind forecasting, or unfavorable wind conditions, resulting in suboptimal turbine performance and reduced energy production.
    Solution: Replace the faulty wind sensor with a new, calibrated unit and recalibrate the turbine control system to accurately capture wind speed and direction. Implement advanced forecasting models and predictive analytics to anticipate and optimize turbine operation based on weather patterns. Conduct regular performance evaluations and data analysis to identify opportunities for operational improvements and maximize energy yield under varying wind conditions.
    """,
    """
    Issue: Excessive blade erosion
    Root Cause: Abrasion or erosion of blade surfaces by airborne particles, such as sand, dust, or salt, leading to loss of aerodynamic efficiency and reduced turbine performance.
    Solution: Apply protective coatings or leading-edge tape to the blade surfaces to minimize erosion and maintain aerodynamic smoothness. Conduct regular inspections and cleaning of the blades to remove accumulated debris and maintain optimal surface condition. Implement environmental monitoring and mitigation measures to minimize exposure to abrasive particles and extend the service life of turbine blades.
    """,
    """
    Issue: Ice buildup on blades
    Root Cause: Freezing precipitation or cold weather conditions causing ice accretion on blade surfaces, leading to imbalanced rotor operation and reduced turbine efficiency.
    Solution: Install passive or active de-icing systems, such as blade heaters, icephobic coatings, or ultrasonic vibration devices, to prevent ice formation or facilitate ice shedding. Conduct regular visual inspections and ice monitoring to detect and address ice buildup promptly. Implement turbine shutdown protocols or operational adjustments during icy conditions to ensure safety and prevent damage to turbine components.
    """,
    """
    Issue: Tower bolt corrosion
    Root Cause: Exposure to saltwater spray, corrosive atmospheres, or moisture ingress, leading to corrosion and degradation of tower bolt materials.
    Solution: Replace corroded bolts and fasteners with marine-grade stainless steel or corrosion-resistant alloys. Apply protective coatings or sealants to vulnerable bolt surfaces to inhibit corrosion and extend service life. Implement regular inspection and maintenance procedures, including bolt torque checks and surface treatments, to prevent future corrosion-related failures and ensure structural integrity of the tower assembly.
    """,
    """
    Issue: Generator overheating
    Root Cause: Inadequate cooling or ventilation of the generator housing, restricted airflow, or malfunctioning cooling fans, resulting in elevated temperatures and thermal stress on generator components.
    Solution: Inspect and clean the generator cooling system, including air vents, filters, and fans, to ensure unobstructed airflow and efficient heat dissipation. Repair or replace malfunctioning cooling fans or temperature sensors to maintain optimal operating temperatures. Implement additional ventilation measures or auxiliary cooling systems to mitigate overheating risks during peak operating conditions. Conduct regular thermal monitoring and performance testing to detect early signs of overheating and prevent generator damage or failures.
    """,
    """
    Issue: Blade delamination
    Root Cause: Material degradation, bonding failure, or manufacturing defects in composite blade structures, leading to separation or delamination of laminate layers.
    Solution: Conduct detailed non-destructive testing, such as ultrasonic or thermographic inspections, to assess the extent of delamination and identify underlying causes. Repair delaminated blade sections using composite patching materials or adhesive bonding techniques to restore structural integrity. Implement quality control measures, such as improved manufacturing processes or enhanced material selection, to prevent future delamination issues. Monitor blade condition regularly and perform preventative maintenance to address any emerging delamination risks promptly.
    """,
    """
    Issue: Tower cracking
    Root Cause: Fatigue stress, material defects, or structural overload, leading to microcracks or fractures in the tower structure.
    Solution: Conduct thorough structural inspections, including ultrasonic or magnetic particle testing, to identify the location and extent of tower cracks. Implement structural reinforcement measures, such as welding repairs, steel plate overlays, or bolted flange connections, to strengthen cracked sections and prevent propagation. Monitor tower stress levels and environmental conditions to optimize operational parameters and minimize fatigue loading. Implement ongoing monitoring and inspection programs to detect and address any emerging crack risks and ensure the long-term integrity and safety of the tower structure.
    """,
    """
    Issue: Gearbox oil contamination
    Root Cause: Seal degradation, gearbox breather system malfunction, or external ingress of contaminants, leading to oil contamination and degradation of gearbox performance.
    Solution: Replace worn-out or damaged seals and gaskets to prevent oil leakage and ingress of contaminants into the gearbox housing. Clean and flush the gearbox internals to remove accumulated debris and contaminants. Install upgraded breather systems or filtration units to maintain clean and dry gearbox environments. Implement regular oil sampling and analysis programs to monitor oil condition and detect early signs of contamination or degradation. Conduct comprehensive gearbox inspections and maintenance procedures to ensure optimal performance and longevity of critical components.
    """,
    """
    Issue: Blade leading edge erosion
    Root Cause: High-velocity airborne particles, such as sand and dust, impacting the leading edge of turbine blades, leading to erosion and reduced aerodynamic efficiency.
    Solution: Install erosion protection tapes or coatings on the leading edge of turbine blades to mitigate abrasive wear. Implement regular inspections and maintenance routines to monitor erosion levels and reapply protective measures as needed. Conduct environmental studies to identify local sources of abrasive particles and implement mitigation strategies, such as vegetation barriers or dust suppression measures, to reduce airborne debris.
    """,
    """
    Issue: Lightning strike damage
    Root Cause: Direct lightning strikes or induced electrical surges during thunderstorms, causing electrical component damage, insulation breakdown, and structural harm to turbine systems.
    Solution: Install lightning protection systems, such as lightning rods, grounding systems, and surge arrestors, to divert and dissipate lightning strikes safely away from turbine structures. Conduct regular inspections and testing of lightning protection measures to ensure effectiveness and compliance with industry standards. Implement lightning detection and early warning systems to enable proactive turbine shutdowns during approaching storms to minimize lightning-related risks.
    """,
    """
    Issue: Pitch system misalignment
    Root Cause: Mechanical wear, hydraulic fluid leakage, or electrical faults in the pitch control system, resulting in irregular blade pitch angles and suboptimal turbine performance.
    Solution: Conduct comprehensive diagnostic tests of the pitch system, including actuator functionality, sensor calibration, and hydraulic pressure checks. Repair or replace faulty components, such as actuators, valves, or position sensors, and recalibrate the pitch control system to ensure precise blade angle adjustments. Implement regular maintenance procedures, including lubrication of moving parts and inspection of hydraulic lines, to prevent future pitch system malfunctions.
    """,
    """
    Issue: Generator bearing failure
    Root Cause: Insufficient lubrication, bearing misalignment, or excessive mechanical loading, leading to premature wear, overheating, and eventual failure of generator bearings.
    Solution: Conduct a thorough inspection of the generator bearings and lubrication system to identify the root cause of failure. Replace worn-out bearings with high-quality, precision-engineered replacements and realign bearing housings to ensure proper alignment and load distribution. Implement enhanced lubrication maintenance practices, including regular oil analysis, filtration, and temperature monitoring, to optimize bearing performance and extend service life. Conduct comprehensive load and stress analysis to identify and mitigate factors contributing to bearing wear and failure.
    """,
    """
    Issue: Tower foundation settling
    Root Cause: Soil consolidation, subsurface water movement, or improper foundation design, leading to uneven settlement and structural instability of turbine tower foundations.
    Solution: Conduct geotechnical investigations, including soil borings, compaction tests, and groundwater monitoring, to assess foundation conditions and identify factors contributing to settlement. Implement corrective measures, such as underpinning, grouting, or soil stabilization techniques, to reinforce and level tower foundations. Monitor foundation settlement regularly using precise leveling surveys or tilt monitoring systems and implement ongoing maintenance and inspection programs to ensure the long-term stability and safety of tower structures.
    """,
    """
    Issue: Gearbox oil contamination
    Root Cause: Seal degradation, gearbox breather system malfunction, or external ingress of contaminants, leading to oil contamination and degradation of gearbox performance.
    Solution: Replace worn-out or damaged seals and gaskets to prevent oil leakage and ingress of contaminants into the gearbox housing. Clean and flush the gearbox internals to remove accumulated debris and contaminants. Install upgraded breather systems or filtration units to maintain clean and dry gearbox environments. Implement regular oil sampling and analysis programs to monitor oil condition and detect early signs of contamination or degradation. Conduct comprehensive gearbox inspections and maintenance procedures to ensure optimal performance and longevity of critical components.
    """,
    """
    Issue: Tower lighting failure
    Root Cause: Electrical component malfunction, wiring damage, or power supply issues, resulting in non-functioning or unreliable obstruction lighting systems on turbine towers.
    Solution: Conduct a comprehensive inspection of tower lighting systems, including lamps, fixtures, control panels, and wiring connections. Replace faulty components, repair damaged wiring, and verify proper grounding and electrical continuity to ensure reliable operation of obstruction lighting. Implement redundant power supply sources, such as backup batteries or emergency generators, to maintain continuous tower illumination during power outages or electrical failures. Conduct regular maintenance checks and functional tests of tower lighting systems to ensure compliance with aviation safety regulations and nighttime visibility requirements.
    """,
    """
    Issue: Rotor imbalance
    Root Cause: Accumulation of ice, snow, or debris on rotor blades, leading to imbalance and vibration during turbine operation.
    Solution: Conduct regular inspections and cleaning of rotor blades to remove accumulated ice, snow, or debris. Implement de-icing systems or heating elements on rotor blades to prevent ice formation or facilitate ice shedding during cold weather conditions. Conduct dynamic balancing of rotor assemblies using specialized equipment to minimize vibration and optimize turbine performance. Implement environmental monitoring and predictive maintenance programs to detect and address rotor imbalance issues before they escalate and impact turbine reliability or safety.
    """,
    """
    Issue: High gearbox temperature
    Root Cause: Inadequate lubrication, bearing misalignment, or excessive mechanical loading, leading to increased friction and heat generation in the gearbox assembly.
    Solution: Conduct a comprehensive inspection of the gearbox internals, including gears, bearings, and lubrication systems, to identify the root cause of overheating. Replace worn-out bearings, realign gear meshes, and replenish gearbox oil with a high-quality, temperature-resistant lubricant. Implement enhanced lubrication maintenance practices, including oil filtration and temperature monitoring, to optimize gearbox performance and extend service life. Conduct regular vibration analysis and thermal imaging inspections to detect early signs of gearbox issues and prevent catastrophic failures.
    """,
    """
    Issue: Tower Corrosion
    Root Cause: Exposure to harsh environmental conditions, including salt spray, moisture, and atmospheric pollutants, leading to corrosion of structural steel components in the turbine tower.
    Solution: Conduct regular visual inspections and non-destructive testing (NDT) of tower surfaces to identify corrosion hotspots and assess structural integrity. Implement corrosion protection measures, such as surface coatings, sacrificial anode systems, or cathodic protection, to mitigate corrosion and extend the service life of tower structures. Develop a comprehensive corrosion management plan, including periodic maintenance painting, surface cleaning, and corrosion monitoring, to prevent progressive degradation of tower assets and ensure long-term reliability and safety.
    """,
    """
    Issue: Brake System Failure
    Root Cause: Wear and tear of brake pads, hydraulic system leaks, electrical faults, or mechanical failures in braking components, leading to loss of braking capability and potential turbine overspeed conditions.
    Solution: Conduct a detailed inspection of brake system components, including pads, calipers, hydraulic hoses, and electrical connections, to identify and rectify any deficiencies or defects affecting system performance. Replace worn-out brake pads, repair hydraulic leaks, and perform system pressure tests to verify proper brake operation and reliability. Implement preventive maintenance procedures, such as lubrication of brake mechanisms and adjustment of brake calipers, to ensure consistent and effective braking under normal and emergency conditions. Develop contingency plans and emergency shutdown procedures to safely stop turbine operation in the event of brake system failures or overspeed events.
    """,
    """
    Issue: Pitch Bearing Wear
    Root Cause: Continuous rotation and cyclic loading of pitch bearings, leading to wear and fatigue of bearing surfaces and eventual degradation of pitch system performance.
    Solution: Conduct periodic inspections and condition monitoring of pitch bearing assemblies to assess wear levels and detect early signs of bearing deterioration, such as increased friction or play in bearing mounts. Implement a proactive bearing maintenance program, including regular lubrication, greasing, and bearing replacement as per manufacturer recommendations or predetermined service intervals. Monitor pitch system performance parameters, such as pitch angle response times and motor currents, to detect abnormal behavior indicative of bearing wear or impending failure. Consider upgrading to high-performance, maintenance-free bearing solutions or implementing remote monitoring systems to improve bearing reliability and reduce maintenance overhead.
    """,
    """
    Issue: Lightning Strike Damage
    Root Cause: Direct or indirect lightning strikes to turbine structures, blades, or electrical systems, leading to physical damage, electrical surges, or component failures.
    Solution: Conduct post-strike inspections and structural integrity assessments to identify and assess lightning strike damage, including blade delamination, surface burns, or electrical system faults. Implement lightning protection measures, such as lightning rods, surge arrestors, or grounding systems, to minimize the risk of future lightning-related damage and ensure safe operation of wind turbines during thunderstorm events. Develop emergency response procedures and contingency plans for lightning strike incidents, including turbine shutdown protocols and inspection criteria for assessing post-strike damage and initiating repair or replacement actions as necessary.
    """,
    """
    Issue: Yaw System Misalignment
    Root Cause: Mechanical wear, misalignment of yaw drive components, sensor inaccuracies, or software errors, leading to deviations from optimal wind direction alignment and reduced turbine performance.
    Solution: Conduct comprehensive inspections and functional tests of yaw system components, including yaw motors, drives, sensors, and controllers, to identify and rectify any issues affecting system alignment and responsiveness. Implement regular calibration procedures for yaw angle sensors and feedback loops to ensure accurate positioning and alignment of turbine nacelles with prevailing wind directions. Perform realignment of yaw drive mechanisms and yaw bearings as necessary to restore proper yaw system functionality and optimize wind energy capture. Develop remote monitoring capabilities and automated yaw control algorithms to facilitate proactive yaw system management and minimize downtime associated with yaw-related faults or misalignments.
    """,
    """
    Issue: Tower Foundation Settlement
    Root Cause: Soil subsidence, inadequate foundation design, or construction deficiencies, leading to uneven settlement or tilting of turbine tower foundations and potential structural integrity risks.
    Solution: Conduct geotechnical investigations and foundation assessments to evaluate soil conditions, bearing capacity, and settlement patterns around turbine tower foundations. Implement remedial measures, such as soil stabilization, grouting, or underpinning, to mitigate foundation settlement and restore structural stability. Monitor foundation settlement using precision leveling surveys, inclinometers, or strain gauges to detect ongoing settlement trends and trigger timely intervention measures. Develop contingency plans and emergency response procedures for addressing foundation settlement risks, including turbine shutdown protocols and structural reinforcement strategies to prevent foundation failures or collapses.
    """,
    """
    Issue: Gearbox Oil Leaks
    Root Cause: Seal degradation, gearbox housing cracks, or excessive mechanical stresses, leading to oil leaks and contamination of gearbox internals, lubricants, and surrounding components.
    Solution: Conduct a detailed inspection of gearbox seals, gaskets, and housing components to identify and repair sources of oil leaks and prevent further fluid loss or contamination. Replace damaged or worn-out seals, repair gearbox housing defects, and tighten fasteners to ensure proper sealing integrity and containment of gearbox lubricants. Implement enhanced oil leak detection and monitoring systems, including drip trays, oil level sensors, or ultraviolet (UV) dye tracing, to identify leaks early and initiate prompt corrective actions. Develop preventive maintenance procedures for gearbox oil seals, including regular inspection, cleaning, and lubrication, to extend seal service life and minimize the risk of oil leakage-related gearbox failures.
    """,
    """
    Issue: Tower Climbing Safety Hazards
    Root Cause: Inadequate safety procedures, equipment deficiencies, or lack of training for personnel performing tower climbing and maintenance activities, leading to increased risk of falls, injuries, or accidents.
    Solution: Conduct comprehensive safety audits and risk assessments of tower climbing operations to identify potential hazards, including fall hazards, electrical risks, and environmental factors, and develop mitigation strategies to address identified risks. Implement rigorous safety training programs for personnel involved in tower climbing and maintenance tasks, including proper use of personal protective equipment (PPE), fall arrest systems, and emergency rescue procedures. Enforce strict adherence to safety protocols and regulatory standards governing tower climbing operations, including pre-climb safety checks, buddy systems, and site-specific hazard awareness training. Conduct regular safety inspections and performance evaluations to monitor compliance with safety guidelines and identify opportunities for continuous improvement in tower climbing safety practices and procedures.
    """,
    """Issue: Tower lighting failure
Root Cause: Electrical component malfunction, wiring damage, or power supply issues, resulting in non-functioning or unreliable obstruction lighting systems on turbine towers.
Solution: Conduct a comprehensive inspection of tower lighting systems, including lamps, fixtures, control panels, and wiring connections. Replace faulty components, repair damaged wiring, and verify proper grounding and electrical continuity to ensure reliable operation of obstruction lighting. Implement redundant power supply sources, such as backup batteries or emergency generators, to maintain continuous tower illumination during power outages or electrical failures. Conduct regular maintenance checks and functional tests of tower lighting systems to ensure compliance with aviation safety regulations and nighttime visibility requirements.
""",
    """Issue: Rotor imbalance
Root Cause: Accumulation of ice, snow, or debris on rotor blades, leading to imbalance and vibration during turbine operation.
Solution: Conduct regular inspections and cleaning of rotor blades to remove accumulated ice, snow, or debris. Implement de-icing systems or heating elements on rotor blades to prevent ice formation or facilitate ice shedding during cold weather conditions. Conduct dynamic balancing of rotor assemblies using specialized equipment to minimize vibration and optimize turbine performance. Implement environmental monitoring and predictive maintenance programs to detect and address rotor imbalance issues before they escalate and impact turbine reliability or safety.
""",
    """Issue: Gearbox overheating
Root Cause: Lubrication system failure, excessive load, or mechanical wear and tear in gearbox components, leading to increased friction and heat generation.
Solution: Conduct thorough inspections of gearbox components, including gears, bearings, and lubrication systems, to identify and rectify any issues causing overheating. Replace worn-out or damaged parts, replenish lubricants, and optimize gear alignment to reduce friction and heat buildup. Implement condition monitoring systems to track gearbox temperature and performance parameters, enabling early detection of potential overheating issues. Develop maintenance schedules for regular gearbox inspections and lubrication servicing to prevent recurrent overheating incidents and ensure long-term gearbox reliability.
""",
    """Issue: Generator malfunction
Root Cause: Electrical component failure, insulation breakdown, or overload conditions in the generator system, resulting in reduced power output or complete shutdown of the turbine.
Solution: Perform detailed inspections and diagnostic tests on generator components, such as stator windings, rotor assembly, and electrical connections, to identify and rectify any malfunctions or damage. Replace faulty components, repair insulation defects, and verify proper grounding and electrical continuity to restore generator functionality. Implement load monitoring and control systems to prevent overloading and ensure safe operating conditions for the generator. Conduct periodic performance tests and efficiency evaluations of the generator system to maintain optimal turbine performance and reliability.
""",
    """Issue: Pitch system failure
Root Cause: Hydraulic system malfunction, sensor errors, or mechanical wear in pitch control mechanisms, leading to ineffective blade pitch adjustment and reduced turbine performance.
Solution: Conduct comprehensive inspections of pitch system components, including hydraulic actuators, pitch bearings, and control sensors, to diagnose and address underlying issues causing system failures. Replace worn-out or damaged parts, recalibrate sensor settings, and perform hydraulic fluid analysis to ensure proper functioning of pitch mechanisms. Implement redundant control systems or backup hydraulic units to mitigate the risk of pitch system failures and maintain operational reliability. Develop preventive maintenance plans for regular lubrication, adjustment, and testing of pitch system components to prevent potential failures and ensure consistent turbine performance.
""",
    """Issue: Tower corrosion
Root Cause: Exposure to harsh environmental conditions, moisture ingress, or inadequate protective coatings on tower surfaces, leading to corrosion and structural deterioration.
Solution: Conduct visual inspections and non-destructive testing (NDT) techniques, such as ultrasonic thickness measurements, to assess the extent of corrosion damage on tower structures. Remove rust, scale, or deteriorated coatings from affected areas and apply corrosion-resistant paints or coatings to inhibit further deterioration. Implement corrosion monitoring systems and cathodic protection methods to prevent corrosion initiation and propagation on tower surfaces. Develop maintenance protocols for regular cleaning, inspection, and recoating of tower structures to prolong service life and ensure structural integrity.
""",
    """Issue: Vibration in drivetrain components
Root Cause: Misalignment, bearing wear, gearbox issues, or unbalanced rotor assemblies, leading to excessive vibration levels in drivetrain components during turbine operation.
Solution: Perform vibration analysis and diagnostic tests on drivetrain components, including gearboxes, shafts, and bearings, to identify root causes of vibration problems. Rectify misalignment issues, replace worn-out bearings, and conduct dynamic balancing of rotor assemblies to reduce vibration levels and prevent damage to critical components. Implement condition monitoring systems equipped with accelerometers and vibration sensors to continuously monitor drivetrain performance and detect abnormal vibration patterns. Develop maintenance schedules for regular lubrication, alignment checks, and vibration analysis to ensure smooth and reliable operation of turbine drivetrains.
""",
    """Issue: Blade erosion or leading edge damage
Root Cause: Exposure to abrasive particles, environmental factors, or aerodynamic forces causing erosion or impact damage to turbine blades, particularly at leading edges.
Solution: Conduct visual inspections and aerodynamic analysis of turbine blades to identify areas of erosion or damage, focusing on leading edges where impacts are most likely to occur. Repair minor damage using composite patching materials or protective coatings to restore aerodynamic efficiency and structural integrity. Implement erosion monitoring systems and weather forecasting tools to anticipate adverse environmental conditions and mitigate potential blade erosion risks. Develop maintenance plans for regular blade inspections, cleaning, and repair activities to minimize downtime and maintain optimal turbine performance.
""",
    """Issue: Yaw system malfunction
Root Cause: Sensor errors, mechanical wear in yaw bearings, or hydraulic system failures, leading to improper alignment of turbine nacelle with wind direction and reduced energy capture efficiency.
Solution: Perform diagnostics and functional tests on yaw system components, including yaw motors, bearings, and control sensors, to identify underlying issues causing system malfunctions. Calibrate sensor settings, replace worn-out bearings, and conduct hydraulic system checks to ensure proper yaw alignment and responsiveness to wind changes. Implement redundancy measures or backup yaw control systems to maintain operational stability and minimize downtime during yaw system failures. Develop maintenance procedures for regular lubrication, inspection, and testing of yaw system components to ensure reliable performance and maximize energy production.
""",
    """Issue: Foundation settlement or subsidence
Root Cause: Soil conditions, inadequate foundation design, or construction defects leading to settlement or subsidence of turbine foundations, compromising structural stability and turbine alignment.
Solution: Conduct geotechnical investigations and structural assessments of turbine foundations to evaluate soil properties, bearing capacity, and foundation integrity. Implement corrective measures such as underpinning, grouting, or soil stabilization techniques to mitigate settlement or subsidence issues and restore foundation stability. Monitor foundation settlement using surveying techniques or automated monitoring systems to detect and address any ongoing subsidence concerns. Develop maintenance protocols for regular inspection and maintenance of turbine foundations, including soil compaction testing and foundation alignment checks, to prevent structural integrity issues and ensure long-term reliability.
""",
    """Issue: Control system errors or anomalies
Root Cause: Software glitches, communication errors, or hardware malfunctions in turbine control systems, leading to operational inefficiencies or safety hazards.
Solution: Conduct diagnostics and software analysis of turbine control systems, including SCADA (Supervisory Control and Data Acquisition) systems, PLCs (Programmable Logic Controllers), and communication interfaces, to identify and rectify errors or anomalies. Update software firmware, reset communication protocols, and conduct system re-calibration to restore normal operation and functionality. Implement cybersecurity measures and redundancy protocols to protect control systems from cyber threats and ensure operational continuity. Develop training programs for maintenance"""
]

# COMMAND ----------

def generate_turbine_data(turbine):
  turbine = int(turbine)
  rd = random.Random()
  rd.seed(turbine)
  damaged = turbine > turbine_count*0.6
  if turbine % 10 == 0:
    print(f"generating turbine {turbine} - damage: {damaged}")
  df = pd.DataFrame()
  damaged_sensors = []
  rd.shuffle(sensors)
  for s in sensors:
    #30% change to have 1 sensor being damaged
    #Only 1 sensor can send damaged metrics at a time to simplify the model. A C and E won't be damaged for simplification
    if damaged and len(damaged_sensors) == 0 and s['name'] not in ["A", "C", "E"]:
      damaged_sensor = rd.randint(1,10) > 5
    else:
      damaged_sensor = False
    if damaged_sensor:
      damaged_sensors.append('sensor_'+s['name'])
    plot = turbine == 0
    df['sensor_'+s['name']] = generate_sensor_data(turbine, s, damaged_sensor, sample_size, plot)

  dg = sdg()
  #Damaged turbine will produce less
  factor = 50 if damaged else 30
  energy = dg.generation_input.add_option(sensor_names="energy", eq="x", initial={"x":0}, step={"x":np.absolute(np.random.randn(sample_size).cumsum()/factor)})
  dg.generate(sample_size, seed=rd.uniform(0,10000))
  #Add some null values in some timeseries to get expectation metrics
  if damaged and rd.randint(0,9) >7:
    n_nulls = int(sample_size*0.005)
    indicies = np.sort(np.random.randint(0, sample_size-1, n_nulls))
    dg.inject(value=None, sensor="energy", index=indicies)

  if plot:
    dg.plot_data()
  df['energy'] = dg.data['energy']

  df.insert(0, 'timestamp', range(current_time, current_time + len(df)*frequency_sec, frequency_sec))
  df['turbine_id'] = str(uuid.UUID(int=rd.getrandbits(128)))
  #df['damaged'] = damaged
  df['abnormal_sensor'] = "ok" if len(damaged_sensors) == 0 else damaged_sensors[0]
  df['maintenance_report'] = df['abnormal_sensor'].apply(lambda x: random.choice(report_list) if x != 'ok' else "N/A")  
  return df

from typing import Iterator
import pandas as pd
from pyspark.sql.functions import pandas_udf, col  

df_schema=spark.createDataFrame(generate_turbine_data(0)) 

def generate_turbine(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
  for pdf in iterator:
    for i, row in pdf.iterrows():
      yield generate_turbine_data(row["id"])

spark_df = spark.range(0, turbine_count).repartition(int(turbine_count/10)).mapInPandas(generate_turbine, schema=df_schema.schema)

# COMMAND ----------

root_folder = folder
folder_sensor = root_folder+'/incoming_data'
spark_df.drop('damaged').drop('abnormal_sensor').orderBy('timestamp').repartition(100).write.mode('overwrite').format('parquet').save(folder_sensor)

#Cleanup meta file to only keep names
def cleanup(folder):
  for f in dbutils.fs.ls(folder):
    if not f.name.startswith('part-00'):
      if not f.path.startswith('dbfs:/Volumes'):
        raise Exception(f"unexpected path, {f} throwing exception for safety")
      dbutils.fs.rm(f.path)
      
cleanup(folder_sensor)

# COMMAND ----------

from faker import Faker
from pyspark.sql.types import ArrayType, FloatType, StringType
import pyspark.sql.functions as F

Faker.seed(0)
faker = Faker()
fake_latlng = F.udf(lambda: list(faker.local_latlng(country_code = 'US')), ArrayType(StringType()))

# COMMAND ----------

rd = random.Random()
rd.seed(0)
folder = root_folder+'/turbine'
(spark_df.select('turbine_id').drop_duplicates()
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.col('fake_lat_long').getItem(0))
   .withColumn('long', F.col('fake_lat_long').getItem(1))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long')
 .orderBy(F.rand()).repartition(1).write.mode('overwrite').format('json').save(folder))

#Add some turbine with wrong data for expectations
fake_null_uuid = F.udf(lambda: None if rd.randint(0,9) > 2 else str(uuid.uuid4()))
df_error = (spark_df.select('turbine_id').limit(30)
   .withColumn('turbine_id', fake_null_uuid())
   .withColumn('fake_lat_long', fake_latlng())
   .withColumn('model', F.lit('EpicWind'))
   .withColumn('lat', F.lit("ERROR"))
   .withColumn('long', F.lit("ERROR"))
   .withColumn('location', F.col('fake_lat_long').getItem(2))
   .withColumn('country', F.col('fake_lat_long').getItem(3))
   .withColumn('state', F.col('fake_lat_long').getItem(4))
   .drop('fake_lat_long').repartition(1).write.mode('append').format('json').save(folder))
cleanup(folder)

folder_status = root_folder+'/historical_turbine_status'
(spark_df.select('turbine_id', 'abnormal_sensor', 'maintenance_report').drop_duplicates()
         .withColumn('start_time', (F.lit(current_time-1000)-F.rand()*2000).cast('int'))
         .withColumn('end_time', (F.lit(current_time+3600*24*30)+F.rand()*4000).cast('int'))
         .repartition(1).write.mode('overwrite').format('json').save(folder_status))
cleanup(folder_status)

# COMMAND ----------

# MAGIC %md 
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/5/52/EERE_illust_large_turbine.gif">

# COMMAND ----------

#see https://blog.enerpac.com/wind-turbine-maintenance-components-strategies-and-tools/
#Get the list of states where our wind turbine are
states = spark.read.json(folder).select('state').distinct().collect()
states = [s['state'] for s in states]
#For each state, we'll generate supply chain parts
part_categories = [{'name': 'blade'}, {'name': 'Yaw drive'}, {'name': 'Brake'}, {'name': 'anemometer'}, {'name': 'controller card #1'}, {'name': 'controller card #2'}, {'name': 'Yaw motor'}, {'name': 'hydraulics'}, {'name': 'electronic guidance system'}]
sensors = [c for c in spark.read.parquet(folder_sensor).columns if "sensor" in c]
parts = []
for p in part_categories:
  for _ in range (1, rd.randint(30, 100)):
    part = {}
    part['EAN'] = None if rd.randint(0,100) > 95 else faker.ean(length=8)
    part['type'] = p['name']
    part['width'] = rd.randint(100,2000)
    part['height'] = rd.randint(100,2000)
    part['weight'] = rd.randint(100,20000)
    part['stock_available'] = rd.randint(0, 5)
    part['stock_location'] =   random.sample(states, 1)[0]
    part['production_time'] = rd.randint(0, 5)
    part['approvisioning_estimated_days'] = rd.randint(30,360)
    part['sensors'] = random.sample(sensors, rd.randint(1,3))
    parts.append(part)
df = spark.createDataFrame(parts)
folder_parts = root_folder+'/parts'
df.repartition(3).write.mode('overwrite').format('json').save(folder_parts)
cleanup(folder_parts)
