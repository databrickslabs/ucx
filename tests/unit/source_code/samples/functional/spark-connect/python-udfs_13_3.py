# ucx[session-state] {"dbr_version": [13, 3], "data_security_mode": "USER_ISOLATION"}
from pyspark.sql.functions import udf, udtf, lit
import pandas as pd


@udf(returnType='int')
def slen(s):
    return len(s)


# ucx[python-udf-in-shared-clusters:+1:1:+1:37] Arrow UDFs require DBR 14.3 LTS or above on UC Shared Clusters
@udf(returnType='int', useArrow=True)
def arrow_slen(s):
    return len(s)


df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
df.select(slen("name"), arrow_slen("name")).show()

slen1 = udf(lambda s: len(s), returnType='int')
# ucx[python-udf-in-shared-clusters:+1:14:+1:68] Arrow UDFs require DBR 14.3 LTS or above on UC Shared Clusters
arrow_slen1 = udf(lambda s: len(s), returnType='int', useArrow=True)

df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))

df.select(slen1("name"), arrow_slen1("name")).show()

df = spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v"))


def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    v = pdf.v
    return pdf.assign(v=v - v.mean())


# ucx[python-udf-in-shared-clusters:+1:0:+1:73] applyInPandas require DBR 14.3 LTS or above on UC Shared Clusters
df.groupby("id").applyInPandas(subtract_mean, schema="id long, v double").show()


class SquareNumbers:
    def eval(self, start: int, end: int):
        for num in range(start, end + 1):
            yield (num, num * num)


# ucx[python-udf-in-shared-clusters:+1:13:+1:69] udtf require DBR 14.3 LTS or above on UC Shared Clusters
square_num = udtf(SquareNumbers, returnType="num: int, squared: int")
square_num(lit(1), lit(3)).show()

from pyspark.sql.types import IntegerType

# ucx[python-udf-in-shared-clusters:+1:0:+1:73] Cannot register Java UDF from Python code on UC Shared Clusters. Use a %scala cell to register the Scala UDF using spark.udf.register.
spark.udf.registerJavaFunction("func", "org.example.func", IntegerType())
