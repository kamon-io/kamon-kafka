import sbt._

//UGLY HACK: https://github.com/sbt/sbt/issues/3618
object PackagingTypePlugin extends AutoPlugin {
  override val buildSettings = {
    sys.props += "packaging.type" -> "jar"
    Nil
  }
}