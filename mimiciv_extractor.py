from sqlalchemy import text
import pandas as pd
from pandas import DataFrame
import pandas.io.sql as psql
from google.cloud import bigquery
from google.oauth2 import service_account

class MimicExtractor:

    def __init__(self, platform="local", user="", password="", database="", host="", schema="", port=None, gcp_credential_file="", gcp_project_id=""):
        '''
        :param str platform: the platform used to connect to the database. SMust be 'local', 'aws' or 'gcp'. Dafault value was set as 'local'.
        :param str user: the database user.
        :param str password: the database user's password.
        :param str database: the name of the database.
        :param str host: the database host name.
        :param str schema: database schema name.
        :param int port: port used to connect to the database.
        :param str gcp_credential_file: [REQUERED WHEN PLATFORM = "GCP"] GCP's Credential file path.
        :param str gcp_project_id: [REQUERED WHEN PLATFORM = "GCP"] GCP's project id.

        '''
        
        self.platform = platform
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.schema = schema
        self.port = port
        self.gcp_credential_file = gcp_credential_file
        self.gcp_project_id = gcp_project_id
        self.gcp_dataset_hosp = "mimiciv_hosp"
        self.gcp_dataset_icu  = "mimiciv_icu"
        
        if platform.lower() == "local":
            self.engine = self.get_engine(user=user, password=password, database=database, host=host, schema=schema, port=port)
        elif platform.lower() == "aws":
            print("Connecting on AWS")
            self.engine = None
        elif platform.lower() == "gcp":
            print("Connecting on GCP")
            
            if not (gcp_credential_file or gcp_project_id):
                raise Exception("You must provede all requised parameters for GCP Platform: gcp_credential_file, gcp_project_id and gcp_dataset")
            
            credentials = service_account.Credentials.from_service_account_file(gcp_credential_file)
            project_id = gcp_project_id
            self.gcp_client = bigquery.Client(credentials=credentials, project=project_id)
        else:
            raise Exception("Platform must be one of the following: 'local', 'aws' or 'gcp'")

    def get_engine(self, user="", password="", database="", host="", schema="", port=None):
        '''
        This function creates a new postgresql engine using SQLAlchemy.

        :param str user: the database user.
        :param str password: the database user's password.
        :param str database: the name of the database.
        :param str host: the database host name.
        :param str schema: database schema name.
        :param int port: port used to connect to the database.
        
        :return: SQLAlchemy Engine Object
        :rtype: sqlalchemy.engine.base.Engine
        '''
        from sqlalchemy import create_engine
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}',
        connect_args={'options': '-csearch_path={}'.format(schema)})
        return engine

    def get_data_from_mimic(self, query='', log=False) -> DataFrame:
        if log: 
            print(f"Running query:\n {query}")

        if self.platform.lower() == "local":
            with self.engine.connect() as conn:
                query_result = psql.read_sql(text(query), conn)
        elif self.platform.lower() == "aws":
            query_result = None
        elif self.platform.lower() == "gcp":
            query_result = self.gcp_client.query(query).to_dataframe()
        else:
            raise Exception("Platform must be one of the following: 'local', 'aws' or 'gcp'")

        return query_result

    def get_patients_age(self, subject_ids: list = None, log=False) -> DataFrame:
        '''
        This function calculates true age of patients at the time of admission.
        NOTE: Patients over 89 years old had their age set to 300 years in MIMIC-IV
        to preserve privacy.

        :param list subject_ids: A list of subject_ids of interest. If no subject_ids is provided, then the search will be performed over all patients present in the database.
        
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        where_clause = ""
        
        if subject_ids:
            subject_ids_str = ','.join(map(str, subject_ids))
            where_clause = f"WHERE p.subject_id IN ({subject_ids_str})"
       
        if (self.platform=='local'):
            age_query = "p.anchor_age + DATE_PART('year', a.admittime) - p.anchor_year"
            table_prefix = "mimiciv_hosp."
        elif (self.platform=='gcp'):
            age_query = "p.anchor_age + EXTRACT(YEAR FROM a.admittime) - p.anchor_year"
            table_prefix = f"physionet-data.{self.gcp_dataset_hosp}."
        elif (self.platform=='aws'):
            age_query=""
            table_prefix=""
        
        query = f"""
        SELECT 
            p.subject_id,
            a.hadm_id,
            a.admittime,
            {age_query} AS age_admission
        FROM {table_prefix}patients p 
        INNER JOIN {table_prefix}admissions a
        ON p.subject_id = a.subject_id
        {where_clause}
        ORDER BY p.subject_id, a.admittime;
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_patients_ethnicity(self, subject_ids=[], log=False):
        '''
        This function returns patients ethnicity by using it's subject IDs.
        
        :param list subject_ids: A list of all subject_ids of interest. If no subject_id is provided, then the search will be performed over all subject_ids present in the database.
        
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        where_clause = ""

        if subject_ids:
            where_clause = f"WHERE subject_id IN ({','.join(map(str, subject_ids))})"

        if (self.platform=='local'):
            table_prefix=""
        elif (self.platform=='gcp'):
            table_prefix = f"physionet-data.{self.gcp_dataset_hosp}."
        elif (self.platform=='aws'):
            table_prefix=""
        
        query = f"""
        SELECT 
            a.hadm_id, a.subject_id, a.race
        FROM {table_prefix}admissions a
        {where_clause};
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_patients_gender(self, subject_ids=[], log=False):
        '''
        This function returns patients gender by using it's subject IDs.
        
        :param list subject_ids: A list of all subject_ids of interest. If no subject_id is provided, then the search will be performed over all subject_ids present in the database.
        
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        where_clause = ""

        if subject_ids:
            where_clause = f"WHERE subject_id IN ({','.join(map(str, subject_ids))})"

        if (self.platform=='local'):
            table_prefix=""
        elif (self.platform=='gcp'):
            table_prefix = f"physionet-data.{self.gcp_dataset_hosp}."
        elif (self.platform=='aws'):
            table_prefix=""
        
        query = f"""
        SELECT 
            p.subject_id, p.gender
        FROM {table_prefix}patients p
         {where_clause};
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_icu_stays_by_diagnosis(self, diagnosis_term='', log=False) -> DataFrame:

        '''
        This function uses a diagnosis term to return all ICU Stays that can relate to that diagnosis.
           
        :param str diagnosis_term: Term used to search for a diagnosis.
           
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''
      
        if self.platform == 'local':
            icu_prefix = "mimiciv_icu."
            hosp_prefix = "mimiciv_hosp."
        elif self.platform == 'gcp':
            icu_prefix  = f"physionet-data.{self.gcp_dataset_icu}."
            hosp_prefix = f"physionet-data.{self.gcp_dataset_hosp}."
        elif self.platform == 'aws':
            icu_prefix=""
            hosp_prefix=""
            
        query = f"""
        SELECT 
            i.subject_id,
            i.hadm_id,
            i.stay_id AS icustay_id,
            i.first_careunit,
            i.last_careunit,
            i.intime,
            i.outtime,
            i.los,
            p.gender,
            p.anchor_age AS age_at_admission,
            p.dod,
            d.icd_code,
            d.icd_version,
            did.long_title
        FROM {icu_prefix}icustays i
        INNER JOIN {hosp_prefix}admissions a
            ON i.hadm_id = a.hadm_id
        LEFT JOIN {hosp_prefix}patients p
            ON i.subject_id = p.subject_id
        LEFT JOIN {hosp_prefix}diagnoses_icd d
            ON i.hadm_id = d.hadm_id
        LEFT JOIN {hosp_prefix}d_icd_diagnoses did
            ON d.icd_code = did.icd_code
            AND d.icd_version = did.icd_version
        WHERE LOWER(did.long_title) LIKE LOWER('%{diagnosis_term}%')
        ORDER BY i.subject_id, i.intime;
        """
        
        return self.get_data_from_mimic(query=query, log=log)
    
        
    def get_icu_sepsis_with_multiple_diagnoses(self, subject_id: int = None, required_diagnosis: str = None, log=False) -> DataFrame:
        '''
        This function returns ICU stays of patients who have multiple diagnoses,
        ensuring that at least one of them is a sepsis-related diagnosis.

        :param int subject_id: A specific subject_id of interest. If none is provided, the search is performed over all patients in the database.
        :param str required_diagnosis: An additional diagnosis term that must also be present among the patient's diagnoses. If none is provided, the function returns all patients who have sepsis along with any other diagnosis.
    
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        if self.platform == 'local':
            hosp_prefix = "mimiciv_hosp."
            icu_prefix = "mimiciv_icu."
            like_operator = "ILIKE"
            agg_func = "ARRAY_AGG"
        elif self.platform == 'gcp':
            hosp_prefix = f"physionet-data.{self.gcp_dataset_hosp}."
            icu_prefix = f"physionet-data.{self.gcp_dataset_icu}."
            like_operator = "LIKE"
            concat_expr = "CONCAT('%', {term}, '%')"
            agg_func = "ARRAY_AGG"
        elif self.platform == 'aws':
            hosp_prefix = "" 
            icu_prefix = "" 
            like_operator = "ILIKE"
            agg_func = "ARRAY_AGG"

        p_subject_id_sql = str(subject_id) if subject_id is not None else 'NULL'
        p_diag_sql = f"'{required_diagnosis}'" if required_diagnosis is not None else 'NULL'

        query = f"""
        WITH icu_patients_diag AS (
            SELECT
                ic.stay_id,
                ic.subject_id,
                ic.hadm_id,
                d.icd_code,
                d.icd_version,
                di.long_title
            FROM {icu_prefix}icustays ic
            JOIN {hosp_prefix}diagnoses_icd d
                ON ic.hadm_id = d.hadm_id
            JOIN {hosp_prefix}d_icd_diagnoses di
                ON d.icd_code = di.icd_code
               AND d.icd_version = di.icd_version
        ),
        sepsis_admissions AS (
            SELECT DISTINCT subject_id, hadm_id
            FROM icu_patients_diag
            WHERE LOWER(long_title) {like_operator} '%sepsis%'
               OR LOWER(long_title) {like_operator} '%septicemia%'
               OR LOWER(long_title) {like_operator} 'septic%'
        ),
        multi_diag_admissions AS (
            SELECT subject_id, hadm_id
            FROM icu_patients_diag
            GROUP BY subject_id, hadm_id
            HAVING COUNT(DISTINCT icd_code) > 1
        )
        SELECT
            ipd.subject_id,
            ipd.hadm_id,
            ipd.stay_id,
            COUNT(DISTINCT ipd.icd_code) AS num_diagnoses,
            {agg_func}(DISTINCT ipd.icd_code) AS icd_codes,
            {agg_func}(DISTINCT ipd.icd_version) AS icd_versions,
            {agg_func}(DISTINCT ipd.long_title) AS long_titles
        FROM icu_patients_diag ipd
        JOIN sepsis_admissions sa
            ON ipd.hadm_id = sa.hadm_id
        JOIN multi_diag_admissions md
            ON ipd.hadm_id = md.hadm_id
        WHERE ( {p_subject_id_sql} IS NULL OR ipd.subject_id = {p_subject_id_sql} )
        AND ( {p_diag_sql} IS NULL OR ipd.long_title {like_operator} '%' || {p_diag_sql} || '%' )
        GROUP BY ipd.subject_id, ipd.hadm_id, ipd.stay_id
        ORDER BY ipd.subject_id, ipd.hadm_id;
        """

        return self.get_data_from_mimic(query=query, log=log)
    
    def get_vital_sign(self, subject_id: int = None, vital_type: str = None, log=False) -> DataFrame:
        '''
        This function returns ICU vital sign measurements from MIMIC-IV database.
        It optionally allows filtering by a specific patient (subject_id) and/or by a specific vital sign type ("Heart Rate", "Temperature", "Bladder Pressure").

        :param int subject_id: Filters results for a specific subject_id. If none is provided, data for all patients will be retrieved.
        :param str vital_type: A keyword used to filter for a specific vital sign label. If none is provided, the function returns all vital signs recorded under the "Vital Signs" category.
    
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        if self.platform == 'local':
            icu_prefix = "mimiciv_icu."
            like_operator = "ILIKE"
        elif self.platform == 'gcp':
            icu_prefix = f"{self.gcp_project_id}.{self.gcp_dataset_icu}."
            like_operator = "LIKE"
        elif self.platform == 'aws':
            icu_prefix = "mimiciv_icu."
            like_operator = "ILIKE" 
            like_operator = "ILIKE"
        
        p_subject_id_sql = str(subject_id) if subject_id is not None else 'NULL'
        p_vital_type_sql = f"'{vital_type}'" if vital_type is not None else 'NULL'

        query = f"""
        SELECT 
            ce.subject_id,
            ce.hadm_id,
            ce.stay_id,
            ce.charttime,
            ce.itemid,
            di.label AS vital_type,
            ce.valuenum,
            ce.valueuom
        FROM {icu_prefix}chartevents ce
        JOIN {icu_prefix}icustays i
            ON ce.stay_id = i.stay_id
        JOIN {icu_prefix}d_items di
            ON ce.itemid = di.itemid
        WHERE di.category {like_operator} '%Vital Signs%'
          AND ( {p_subject_id_sql} IS NULL OR ce.subject_id = {p_subject_id_sql} )
          AND ( {p_vital_type_sql} IS NULL OR di.label {like_operator} '%' || {p_vital_type_sql} || '%' )
        ORDER BY ce.subject_id, ce.charttime;
        """

        return self.get_data_from_mimic(query=query, log=log)
    
    def get_mechanical_ventilation(self, subject_id: int = None, log=False) -> DataFrame:
        '''
        This function returns continuous periods of mechanical ventilation for ICU patients, following the gap-based approach used in MIMIC-IV research. Ventilation events are
        grouped into episodes whenever consecutive charted events occur within a 14-hour interval.
        For each ventilation episode, the function computes its start time, end time, and total duration in hours.

        :param int subject_id: Filters results for a specific subject_id. If none is provided, the function returns mechanical ventilation episodes for all patients.
    
        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        if self.platform == 'local':
            icu_prefix = "mimiciv_icu."
            interval_14h = "INTERVAL '14 hours'"
            time_diff_hours = "EXTRACT(EPOCH FROM (MAX(charttime) - MIN(charttime))) / 3600"
            like_operator = "ILIKE"
        elif self.platform == 'gcp':
            icu_prefix = f"{self.gcp_project_id}.{self.gcp_dataset_icu}."
            interval_14h = "INTERVAL 14 HOUR"
            time_diff_hours = "TIMESTAMP_DIFF(MAX(charttime), MIN(charttime), HOUR)"
            like_operator = "LIKE"
        elif (self.platform=='aws'):
            icu_prefix = "mimiciv_icu."
        interval_14h = "INTERVAL '14 hours'"
        time_diff_hours = "EXTRACT(EPOCH FROM (MAX(charttime) - MIN(charttime))) / 3600"
        like_operator = "ILIKE"

        p_subject_id_sql = str(subject_id) if subject_id is not None else 'NULL'

        query = f"""
        WITH mech_vent_events AS (
            SELECT
                ce.subject_id,
                ce.hadm_id,
                ce.stay_id,
                ce.charttime
            FROM
                {icu_prefix}chartevents ce
            INNER JOIN
                {icu_prefix}d_items di ON ce.itemid = di.itemid
            WHERE
                LOWER(di.label) {like_operator} 'vent%'
                AND ( {p_subject_id_sql} IS NULL OR ce.subject_id = {p_subject_id_sql} )
        ),
        mech_vent_seq AS (
            SELECT
                subject_id,
                hadm_id,
                stay_id,
                charttime,
                LEAD(charttime) OVER (
                    PARTITION BY subject_id, stay_id
                    ORDER BY charttime
                ) AS next_charttime
            FROM
                mech_vent_events
        ),
        mech_vent_boundaries as(
            SELECT
                *,
                CASE
                    WHEN next_charttime IS NULL
                    OR next_charttime > charttime + {interval_14h} THEN 1
                    ELSE 0
                END AS boundary
            FROM
                mech_vent_seq
        ),
        vent_groups AS (
            select *,
                SUM(boundary) OVER (
                    PARTITION BY subject_id, stay_id
                    ORDER BY charttime
                ) AS grp
            FROM
                mech_vent_boundaries
        )
        SELECT 
            subject_id,
            hadm_id,
            stay_id,
            MIN(charttime) AS ventilation_start,
            MAX(charttime) AS ventilation_end,
            ROUND(
                {time_diff_hours},
                2
            ) AS ventilation_hours
        FROM
            vent_groups
        GROUP BY
            subject_id,
            hadm_id,
            stay_id,
            grp
        HAVING
            ROUND(
                {time_diff_hours},
                2
            ) > 0
        ORDER BY subject_id, ventilation_start;
        """

        return self.get_data_from_mimic(query=query, log=log)

    def get_icu_mortality(self, limit_days: int = None, subject_id: int = None, log=False) -> DataFrame:
        '''
        This function returns mortality information for ICU patients who died, based on data from MIMIC-IV database. It identifies deaths using hospital_expire_flag or the presence of a date of death (dod). When a time limit is provided, the 
        function returns only patients whose death occurred within the specified number of days after ICU admission.

        :param int limit_days: Optional time window (in days) used to filter deaths that occurred within the specified number of days after ICU admission. If none is provided, the function returns all ICU mortality cases.
        :param int subject_id: Filters results for a specific subject_id. If none is provided, the function returns mortality information for all deceased ICU patients.

        :return: Returns a DataFrame with the data returned by the query.
        :rtype: DataFrame
        '''

        if self.platform == 'local':
            hosp_prefix = "mimiciv_hosp."
            icu_prefix = "mimiciv_icu."
            date_diff_func = "DATE_PART('day', p.dod - i.intime)"
            concat_func = "CONCAT"
        elif self.platform == 'gcp':
            hosp_prefix = f"physionet-data.{self.gcp_dataset}."
            icu_prefix = f"physionet-data.{self.gcp_dataset_icu}."
            date_diff_func = "DATE_DIFF(p.dod, i.intime, DAY)"
            concat_func = "CONCAT"
        elif self.platform == 'aws':
            hosp_prefix = "mimiciv_hosp."
            icu_prefix = "mimiciv_icu."
            date_diff_func = "DATE_PART('day', p.dod - i.intime)"
            concat_func = "CONCAT"

        p_limit_days_sql = str(limit_days) if limit_days is not None else 'NULL'
        p_subject_id_sql = str(subject_id) if subject_id is not None else 'NULL'

        query = f"""
        WITH mortality_icu AS (
            SELECT
                i.subject_id,
                i.hadm_id,
                i.stay_id, 
                a.hospital_expire_flag,
                p.dod,
                i.intime AS icustay_admittime,
                CASE
                    WHEN a.hospital_expire_flag = 1 OR p.dod IS NOT NULL THEN 1
                    ELSE 0
                END AS mortality_flag,
                CASE
                    WHEN p.dod IS NOT NULL THEN
                        {date_diff_func}
                    ELSE NULL
                END AS mortality_days
            FROM {icu_prefix}icustays i
            INNER JOIN {hosp_prefix}admissions a
                ON i.hadm_id = a.hadm_id
            LEFT JOIN {hosp_prefix}patients p
                ON i.subject_id = p.subject_id
            -- Filtro por subject_id adicionado na CTE
            WHERE ( {p_subject_id_sql} IS NULL OR i.subject_id = {p_subject_id_sql} )
        )
        SELECT
            m.subject_id,
            m.hadm_id,
            m.stay_id,
            m.icustay_admittime,
            m.dod,
            m.mortality_days,
            CASE
                WHEN m.mortality_flag = 1 THEN 
                    CASE
                        WHEN {p_limit_days_sql} IS NOT NULL AND m.mortality_days <= {p_limit_days_sql} THEN 
                            {concat_func}('ICU Mortality within ', {p_limit_days_sql}, ' days')
                        WHEN {p_limit_days_sql} IS NOT NULL AND m.mortality_days > {p_limit_days_sql} THEN 
                            {concat_func}('ICU Mortality beyond ', {p_limit_days_sql}, ' days')
                        ELSE 'ICU Patient Death'
                    END
                ELSE 'ICU Patient No Death'
            END AS mortality_status
        FROM mortality_icu m
        WHERE
            m.mortality_flag = 1
            AND (
                {p_limit_days_sql} IS NULL 
                OR m.mortality_days <= {p_limit_days_sql}
            )
        ORDER BY m.mortality_days NULLS LAST;
        """

        return self.get_data_from_mimic(query=query, log=log)