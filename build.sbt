lazy val scalaLongVersion = "2.12.17"
lazy val Version = "1.0.0"
lazy val appName = "dqchecks"
lazy val sparkVersion = "3.2.1"

lazy val commonSettings = Seq(
  organization := "com.gdp",
  version := Version,
  scalaVersion := scalaLongVersion,
//  crossScalaVersions ++= Seq("2.13.8", scalaLongVersion),
  name := "dqchecks"
)

//logLevel in assembly := Level.Debug
assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith "module-info.class" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

val dockerRegistry = "...azurecr.io"

lazy val dockerSettings = Seq(
  Docker / version := Version,
  dockerRepository := Some(dockerRegistry),
  Docker / packageName := "dqchecks",
  dockerBaseImage := "hseeberger/scala-sbt:8u312_1.6.2_3.1.1", //openjdk:8-jdk-alpine?
  Docker / daemonUser := "sbtuser",
)

lazy val root = (project in file("."))
  .settings(commonSettings: _*)
  .enablePlugins(JavaServerAppPackaging, LinuxPlugin, UpstartPlugin, DockerPlugin)
  .settings(commonSettings: _*)
  .settings(dockerSettings)
  .settings(
    //    dockerBuildOptions += "--no-cache",
    name := appName,
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    Test / logBuffered := false,
    assembly / mainClass := Some("com.gdp.DQJob"),

      libraryDependencies ++= Seq(
        "ch.qos.logback"   %  "logback-classic"  % "1.4.6",
        "org.apache.spark" %% "spark-core"       % sparkVersion % Provided,
        "org.apache.spark" %% "spark-sql"        % sparkVersion % Provided,
        "org.apache.spark" %% "spark-streaming"  % sparkVersion % Provided,
        "com.databricks"   %% "dbutils-api"      % "0.0.6",
        "com.typesafe"     %  "config"           % "1.4.2",
        "com.typesafe.slick" %% "slick-hikaricp" % "3.4.1",
        "org.postgresql"     % "postgresql"      % "42.5.4",
        "com.typesafe.slick" %% "slick"          % "3.4.1",
        "com.amazon.deequ" %  "deequ"            % "2.0.1-spark-3.2" excludeAll(
          ExclusionRule(organization = "ch.qos.logback"),
          ExclusionRule(organization = "org.apache.spark")
      ),
      "org.specs2"         %% "specs2-core"         % "4.19.2"        % Test,
      "org.scalatest"      %% "scalatest"           % "3.2.15"        % Test,
      "org.mockito"        % "mockito-core"         % "5.2.0"         % Test,
        "org.scalatestplus" %% "mockito-4-6"        % "3.2.15.0"      % Test
    )
  )

scalacOptions ++= Seq(
    "-deprecation",         // emit warning and location for usages of deprecated APIs
//    "-explain",             // explain errors in more detail
//    "-explain-types",       // explain type errors in more detail
//    "-print-lines",         // show source code line numbers.
//    "-unchecked",           // enable additional warnings where generated code depends on assumptions
//    "-Ykind-projector",     // allow `*` as wildcard to be compatible with kind projector
    "-encoding", "UTF-8",
    "-feature",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-language:postfixOps",
//    "-Xfatal-warnings",
    "-Xmigration"           // warn about constructs whose behavior may have changed since version
)

