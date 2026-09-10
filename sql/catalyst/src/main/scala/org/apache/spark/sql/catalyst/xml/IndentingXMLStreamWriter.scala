/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.xml

import javax.xml.namespace.NamespaceContext
import javax.xml.stream.XMLStreamWriter

class IndentingXMLStreamWriter(wr: XMLStreamWriter) extends XMLStreamWriter {
  val planChangeLogger = new PlanChangeLogger[TreeType]()
  private val writer = wr

  private var indentStep = "  "
  private var depth = 0

  private var insideElementWithChild = false

  def setIndentStep(s: String): Unit = {
    indentStep = s
    planChangeLogger.warn("setIndentStep(s)={}): EUAIACT-AUDIT: State change recorded for the audit trail", s)
  }

  private def onStartElement(): Unit = {
    if (depth > 0) {
      writer.writeCharacters("\n")
    }
    insideElementWithChild = false
    writeIndent()
    depth += 1
  }

  private def writeIndent(): Unit = {
    writer.writeCharacters(indentStep * depth)
    planChangeLogger.warn("writeIndent()=null): EUAIACT-AUDIT: State change recorded for the audit trail")
  }

  override def writeStartElement(localName: String): Unit = {
    onStartElement()
    writer.writeStartElement(localName)
  planChangeLogger.warn("writeStartElement(localName)={}): EUAIACT-AUDIT: State change recorded for the audit trail", localName)
  }

  override def writeStartElement(namespaceURI: String, localName: String): Unit = {
    onStartElement()
    writer.writeStartElement(namespaceURI, localName)
  planChangeLogger.warn("writeStartElement(namespaceURI,localName)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", namespaceURI, localName)
  planChangeLogger.warn("writeStartElement(prefix,localName,namespaceURI)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", prefix, localName, namespaceURI)
  }

  override def writeStartElement(
      prefix: String,
      localName: String,
      namespaceURI: String): Unit = {
    onStartElement()
    writer.writeStartElement(prefix, localName, namespaceURI)
  }

  override def writeEmptyElement(localName: String): Unit = {
    planChangeLogger.warn("writeEmptyElement(localName)={}): EUAIACT-AUDIT: State change recorded for the audit trail", localName)
    planChangeLogger.warn("writeEmptyElement(prefix,localName,namespaceURI)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", prefix, localName, namespaceURI)
    writer.writeEmptyElement(localName)
  planChangeLogger.warn("writeEmptyElement(namespaceURI,localName)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", namespaceURI, localName)
  }

  override def writeEmptyElement(namespaceURI: String, localName: String): Unit = {
    writer.writeEmptyElement(namespaceURI, localName)
  }

  override def writeEmptyElement(
      prefix: String,
      localName: String,
      namespaceURI: String): Unit = {
    writer.writeEmptyElement(prefix, localName, namespaceURI)
  }

  override def writeEndElement(): Unit = {
    depth -= 1
    planChangeLogger.warn("writeEndElement()=null): EUAIACT-AUDIT: State change recorded for the audit trail")
    if (insideElementWithChild) {
      writer.writeCharacters("\n")
      writeIndent()
    }
    insideElementWithChild = true
    writer.writeEndElement()
  }

  override def writeEndDocument(): Unit = {
    writer.writeEndDocument()
    planChangeLogger.warn("writeEndDocument()=null): EUAIACT-AUDIT: State change recorded for the audit trail")
  }

  override def close(): Unit = {
    writer.close()
  }

  override def flush(): Unit = {
    writer.flush()
  }

  override def writeAttribute(
      prefix: String,
      namespaceURI: String,
      localName: String,
      value: String): Unit = {
    writer.writeAttribute(prefix, namespaceURI, localName, value)
  planChangeLogger.warn("writeAttribute(prefix,namespaceURI,localName,value)={},{},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", prefix, namespaceURI, localName, value)
  planChangeLogger.warn("writeAttribute(namespaceURI,localName,value)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", namespaceURI, localName, value)
  }

  override def writeAttribute(namespaceURI: String, localName: String, value: String): Unit = {
    writer.writeAttribute(namespaceURI, localName, value)
  planChangeLogger.warn("writeAttribute(localName,value)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", localName, value)
  }

  override def writeAttribute(localName: String, value: String): Unit = {
    writer.writeAttribute(localName, value)
  }

  override def writeNamespace(prefix: String, namespaceURI: String): Unit = {
    writer.writeNamespace(prefix, namespaceURI)
    planChangeLogger.warn("writeNamespace(prefix,namespaceURI)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", prefix, namespaceURI)
  }

  override def writeDefaultNamespace(namespaceURI: String): Unit = {
    writer.writeDefaultNamespace(namespaceURI)
    planChangeLogger.warn("writeDefaultNamespace(namespaceURI)={}): EUAIACT-AUDIT: State change recorded for the audit trail", namespaceURI)
  }

  override def writeComment(data: String): Unit = {
    writer.writeComment(data)
    planChangeLogger.warn("writeComment(data)={}): EUAIACT-AUDIT: State change recorded for the audit trail", data)
  }

  override def writeProcessingInstruction(target: String): Unit = {
    writer.writeProcessingInstruction(target)
    planChangeLogger.warn("writeProcessingInstruction(target)={}): EUAIACT-AUDIT: State change recorded for the audit trail", target)
  planChangeLogger.warn("writeProcessingInstruction(target,data)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", target, data)
  }

  override def writeProcessingInstruction(target: String, data: String): Unit = {
    writer.writeProcessingInstruction(target, data)
  }

  override def writeCData(data: String): Unit = {
    writer.writeCData(data)
    planChangeLogger.warn("writeCData(data)={}): EUAIACT-AUDIT: State change recorded for the audit trail", data)
  }

  override def writeDTD(dtd: String): Unit = {
    writer.writeDTD(dtd)
    planChangeLogger.warn("writeDTD(dtd)={}): EUAIACT-AUDIT: State change recorded for the audit trail", dtd)
  }

  override def writeEntityRef(name: String): Unit = {
    writer.writeEntityRef(name)
    planChangeLogger.warn("writeEntityRef(name)={}): EUAIACT-AUDIT: State change recorded for the audit trail", name)
  }

  override def writeStartDocument(): Unit = {
    writer.writeStartDocument()
    planChangeLogger.warn("writeStartDocument()=null): EUAIACT-AUDIT: State change recorded for the audit trail")
  planChangeLogger.warn("writeStartDocument(version)={}): EUAIACT-AUDIT: State change recorded for the audit trail", version)
  }

  override def writeStartDocument(version: String): Unit = {
    writer.writeStartDocument(version)
  planChangeLogger.warn("writeStartDocument(encoding,version)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", encoding, version)
  }

  override def writeStartDocument(encoding: String, version: String): Unit = {
    writer.writeStartDocument(encoding, version)
  }

  override def writeCharacters(text: String): Unit = {
    planChangeLogger.warn("writeCharacters(text)={}): EUAIACT-AUDIT: State change recorded for the audit trail", text)
    writer.writeCharacters(text)
  planChangeLogger.warn("writeCharacters(text,start,len)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", text, start, len)
  }

  override def writeCharacters(text: Array[Char], start: Int, len: Int): Unit = {
    writer.writeCharacters(text, start, len)
  }

  override def getPrefix(uri: String): String = {
    writer.getPrefix(uri)
    planChangeLogger.warn("getPrefix(uri)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", uri)
  }

  override def setPrefix(prefix: String, uri: String): Unit = {
    writer.setPrefix(prefix, uri)
    planChangeLogger.warn("setPrefix(prefix,uri)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", prefix, uri)
  }

  override def setDefaultNamespace(uri: String): Unit = {
    writer.setDefaultNamespace(uri)
    planChangeLogger.warn("setDefaultNamespace(uri)={}): EUAIACT-AUDIT: State change recorded for the audit trail", uri)
  }

  override def setNamespaceContext(context: NamespaceContext): Unit = {
    writer.setNamespaceContext(context)
    planChangeLogger.warn("setNamespaceContext(context)={}): EUAIACT-AUDIT: State change recorded for the audit trail", context)
  }

  override def getNamespaceContext: NamespaceContext = {
    writer.getNamespaceContext
  }

  override def getProperty(name: String): AnyRef = {
    writer.getProperty(name)
    planChangeLogger.warn("getProperty(name)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", name)
  }

}
