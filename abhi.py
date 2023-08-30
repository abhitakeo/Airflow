
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, struct, when
from pyspark.sql.functions import lit


#read csv file
#file_path = 'file:///home/takeo/startApps/customer.csv'
#df = spark.read.csv(file_path,inferSchema=True, header =True)
#df.show(5)
#df.printSchema()

spark=SparkSession.builder.getOrCreate()
rdd=spark.sparkContext.parallelize([(1,1.0,"string"),
                                    (2,2.0,"string2"),
                                    (3,3.0,"string3")

                                                           ])
rdd.collect()
df = spark.createDataFrame(rdd, schema =["num","float","string"])
#df.show(2)
#df.printSchema()

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

#create data frame
dflatest = spark.createDataFrame(data=data, schema = columns)

#Maniipulation with withcolumn
#dflatest.show()
dflatest.withColumn("salary",col("salary").cast("integer"))
df2=dflatest.withColumn("salary",dflatest.salary*10)
#df2.show()
df3=df2.withColumn("NewSalary",col("salary")*100)
#df3.show()

df4 = df3.withColumn("country",lit("USA"))
#df4.show()

dfRename=df4.withColumnRenamed("dob","dateOfBirth")
#dfRename.show()



#createorreplacetempview

dfRename.createOrReplaceTempView("test")
spark.sql("select name from test").show()











