package kamon.kafka.streams.instrumentation

import java.io.{BufferedWriter, File, FileWriter}
import java.time.Duration

import kamon.tag.Tag
import kamon.trace.Span

object DotFileGenerator {

  def dumpToDotFile(filename: String, spans: List[Span.Finished]): Unit = {
    val dotText = toDotString(spans)
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(dotText)
    bw.close()
  }

  def toDotString(spans: List[Span.Finished]): String = {
    def getLabel(s: Span.Finished) = {
      s"""<
         | <table border="0" cellborder="0" cellspacing="0">
         | <tr><td><b>${s.operationName}</b></td></tr>
         | <tr><td>start: ${s.from}</td></tr>
         | <tr><td>duration: ${Duration.between(s.from, s.to).toMillis} ms</td></tr>
         | ${s.tags.iterator.map(t => s"<tr><td>${t.key}=${Tag.unwrapValue(t)}</td></tr>").mkString("\n")}
         | </table>
         |>""".stripMargin
    }

    def createNodes = {
      spans.map(s => s""" "${s.id.string}" [label=""" + getLabel(s) + """];""").mkString("\n")
    }

    def getParent(s: Span.Finished) = {
      s.parentId.string match {
        case "" => "start"
        case s => s
      }
    }

    def createEdges = {
      spans.map(s => s""" "${getParent(s)}" -> "${s.id.string}";""").mkString("\n")
    }

    s"""
       |digraph G {
       | $createNodes
       | $createEdges
       |}
       |""".stripMargin
  }
}

