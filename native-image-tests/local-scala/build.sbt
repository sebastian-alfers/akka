import scala.collection.Seq

name := "local-scala"
version := "1.0"
scalaVersion := "2.13.12"
resolvers += "Akka library repository".at("https://repo.akka.io/maven")

// Note: this default isn't really used anywhere so not important to bump
lazy val akkaVersion = sys.props.getOrElse("akka.version", "2.9.1")

// useful for investigations: sbt nativeImageRunAgent

fork := true

// GraalVM native image build
enablePlugins(NativeImagePlugin)
nativeImageJvm := "graalvm-community"
nativeImageVersion := "21.0.2"
nativeImageOptions := Seq(
  "--no-fallback",
  "--verbose")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.13",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test)