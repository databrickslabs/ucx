import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

val readableMounts = dbutils.fs.mounts.filter(_.mountPoint.startsWith("/mnt")).par.map { mount =>
    try {
        Await.result(Future {
            dbutils.fs.ls(mount.mountPoint)
            (mount.mountPoint.replace("/mnt/", "").stripSuffix("/"),mount.source)
        }, 10.second)
    } catch {
        case _ : Throwable => (null, mount.source);
    }
  }.seq.filter {
    mount => mount._1 != null
  } toMap

val mapper = new ObjectMapper()
mapper.registerModule(DefaultScalaModule)
mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

println(mapper.writeValueAsString(readableMounts));