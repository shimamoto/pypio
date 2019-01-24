name := "akka-simple-cluster"
version := "0.0.1-SNAPSHOT"
scalaVersion := "2.12.8"

resolvers += Resolver.bintrayRepo("tanukkii007", "maven")

val akkaVersion = "2.5.19"
val akkaHttpVersion = "10.1.7"
val akkaManagementVersion = "0.20.0"

libraryDependencies ++=Seq(
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % akkaHttpVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-http" % akkaManagementVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % akkaManagementVersion,
  "com.lightbend.akka.discovery" %% "akka-discovery-kubernetes-api" % akkaManagementVersion,
  "com.lightbend.akka.discovery" %% "akka-discovery-dns" % akkaManagementVersion,
  "com.github.TanUkkii007" %% "akka-cluster-custom-downing" % "0.0.12"
)

enablePlugins(JavaServerAppPackaging, DockerPlugin)
dockerBaseImage := "openjdk:8"
dockerUsername := Some("shimamoto")
