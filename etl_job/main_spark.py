from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
import json
class US_Accident_Analysis:

    def __init__(self, config_dict):
        """
        Intialize the dataframe for all input files.
        :param config_dict: input and output files path.
        """
        self.df_charges = self.extract_data(spark, config_dict['input_file_path']['Charges'])
        self.df_damages = self.extract_data(spark, config_dict['input_file_path']['Damages'])
        self.df_endorse = self.extract_data(spark, config_dict['input_file_path']['Endorse'])
        self.df_primary_person = self.extract_data(spark, config_dict['input_file_path']['Primary_Person'])
        self.df_units = self.extract_data(spark, config_dict['input_file_path']['Units'])
        self.df_restrict = self.extract_data(spark, config_dict['input_file_path']['Restrict'])

    def count_male_crashes_ana_1(self):
        """
        Finds the number of crashes (accidents) in which number of persons killed are male.
        :return: dataframe count (total no. of crashes).
        """
        df = self.df_primary_person.filter(col("PRSN_GNDR_ID") == "MALE")

        self.write_data(df, config_dict['output_file_path']['Analysis_1'])

        return df.count()

    def count_two_wheeler_crashes_ana_2(self):
        """
        Finds count of two wheelers booked for crashes.
        :return: dataframe count (total 2 wheeler crashes).
        """
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))

        self.write_data(df, config_dict['output_file_path']['Analysis_2'])

        return df.count()

    def highest_female_accident_states_ana_3(self):
        """
        Finds state with highest female accidents.
        :return: list of states with highest female accidents.
        """
        df = self.df_primary_person.filter(col("PRSN_GNDR_ID") == "FEMALE"). \
            groupby("DRVR_LIC_STATE_ID").count(). \
            orderBy(col("count").desc())

        self.write_data(df, config_dict['output_file_path']['Analysis_3'])
        
        return [ele for ele in df.select("DRVR_LIC_STATE_ID").first()]

    def top_vehicle_contrib_injuries_ana_4(self):
        """
        Finds Top 5th to 15th VEH_MAKE_IDs contributing largest number of injuries including death.
        :return: list of Top 5th to 15th VEH_MAKE_IDs with largest injuries including death.
        """
        df = self.df_units.filter(col("VEH_MAKE_ID") != "NA"). \
            withColumn('TOT_CASUALT_CNT', col("TOT_INJRY_CNT") + col("DEATH_CNT")). \
            groupby("VEH_MAKE_ID").sum("TOT_CASUALT_CNT"). \
            withColumnRenamed("sum(TOT_CASUALT_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
            orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())
        df_top_5_to_15 = df.limit(15).subtract(df.limit(5))

        self.write_data(df, config_dict['output_file_path']['Analysis_4'])

        return [ele[0] for ele in df_top_5_to_15.select("VEH_MAKE_ID").collect()]

    def top_ethnic_usergroup_bodystyle_ana_5(self):
        """
        Finds top ethnic user group of each unique body style involved in crashes.
        :return: dataframe.
        """
        window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = self.df_units.join(self.df_primary_person, self.df_units.CRASH_ID == self.df_primary_person.CRASH_ID, 'inner'). \
            filter(~self.df_units.VEH_BODY_STYL_ID.isin(["NA", "NOT REPORTED","OTHER  (EXPLAIN IN NARRATIVE)","UNKNOWN"])). \
            filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("row_num", row_number().over(window_spec)).where(col("row_num") == 1).\
            drop("row_num", "count")

        self.write_data(df, config_dict['output_file_path']['Analysis_5'])

        return df.show(truncate=False)

    def top_5_zipcodes_alcohols_crash_ana_6(self):
        """
        Finds top 5 Zip Codes with highest crashes having alcohols as the contributing factor.
        :return: List of top 5 Zip Codes with highest crashes having alcohols.
        """
        df = self.df_units.join(self.df_primary_person, self.df_units.CRASH_ID == self.df_primary_person.CRASH_ID, 'inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        
        self.write_data(df, config_dict['output_file_path']['Analysis_6'])

        return [ele[0] for ele in df.collect()]

    def no_damage_crash_ids_ana_7(self):
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
        and car avails Insurance.
        :return: List of crash ids.
        """
        df = self.df_damages.join(self.df_units,  on=["CRASH_ID"], how='inner'). \
            filter(
            ((self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &(~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))) 
            |((self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &(~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"])))
             ).\
            filter(self.df_damages.DAMAGED_PROPERTY.isin(["NONE","NONE1"])). \
            filter(self.df_units.FIN_RESP_TYPE_ID.isin(["PROOF OF LIABILITY INSURANCE","CERTIFICATE OF SELF-INSURANCE","LIABILITY INSURANCE POLICY"]))
               

        self.write_data(df, config_dict['output_file_path']['Analysis_7'])

        return [ele[0] for ele in df.collect()]

    def top_5_vehicle_brands_ana_8(self):
        """
        Determines the Top 5 Vehicle Makes where drivers charged with speeding offences, has licensed Drivers, 
        uses top 10 used vehicle colours and  car licensed with Top 25 states with highest number of offences.
        :return List of Top 5 Vehicle Makers.
        """
        top10_veh_color_df = self.df_units.filter(col("VEH_COLOR_ID") != "NA").\
                    groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10)
        top_10_veh_colors_list = [ele[0] for ele in top10_veh_color_df.collect()]

        top_25_state_df = self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).\
                    groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25)
        top_25_state_list = [ele[0] for ele in top_25_state_df.collect()]
        
        df = self.df_charges.join(self.df_primary_person,self.df_charges.CRASH_ID == self.df_primary_person.CRASH_ID, 'inner'). \
            join(self.df_units, self.df_charges.CRASH_ID == self.df_units.CRASH_ID, 'inner'). \
            filter(self.df_charges.CHARGE.contains("SPEED")). \
            filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["COMMERCIAL DRIVER LIC.","DRIVER LICENSE"])). \
            filter(self.df_units.VEH_COLOR_ID.isin(top_10_veh_colors_list)). \
            filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5)

        self.write_data(df, config_dict['output_file_path']['Analysis_8'])

        return [row[0] for row in df.collect()]
    
    def extract_data(self,spark,file_path):
        """Load data from csv file format.
        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
        df = spark.read.option("inferSchema", "true").option("header","true").csv(file_path)
        return df

    def write_data(self,df,file_path):
        """Collect data locally and write to CSV.
        :param df: DataFrame to print.
        :param file_path: output_files path.
        :return: None.
        """
        df.coalesce(1).write.format("csv").mode('overwrite').option("header", "true").save(file_path)
        return None


# entry point for PySpark ETL application
if __name__ == '__main__':
    
    #intializing Spark Session
    spark = SparkSession \
        .builder \
        .appName("US_Accident_Analysis") \
        .getOrCreate()
    
    # Reading Config files and loading to config dictionary
    config_path="./configs/config.json"
    with open(config_path, 'r') as config_file:
        config_dict = json.load(config_file)

    # setting spark log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    us_acc_obj = US_Accident_Analysis(config_dict)

    # 1. Find the number of crashes (accidents) in which number of persons killed are male?
    print("Analysis 1:", us_acc_obj.count_male_crashes_ana_1())

    # 2. How many two-wheelers are booked for crashes?
    print("Analysis 2:", us_acc_obj.count_two_wheeler_crashes_ana_2())

    # 3. Which state has the highest number of accidents in which females are involved?
    print("Analysis 3:", us_acc_obj.highest_female_accident_states_ana_3())

    # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print("Analysis 4:", us_acc_obj.top_vehicle_contrib_injuries_ana_4())

    # 5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("Analysis 5:")
    us_acc_obj.top_ethnic_usergroup_bodystyle_ana_5()

    # 6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the
    # contributing factor to a crash (Use Driver Zip Code)
    print("Analysis 6:", us_acc_obj.top_5_zipcodes_alcohols_crash_ana_6())

    # 7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
    # and car avails Insurance
    print("Analysis 7:", us_acc_obj.no_damage_crash_ids_ana_7())

    # 8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
    # Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offences (to be deduced from the data)
    print("Analysis 8:", us_acc_obj.top_5_vehicle_brands_ana_8())
    
    spark.stop()


