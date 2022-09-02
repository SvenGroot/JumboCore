<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:msxsl="urn:schemas-microsoft-com:xslt" exclude-result-prefixes="msxsl"
    xmlns="http://www.w3.org/1999/xhtml"
>
  <!-- $Id -->
  <xsl:output method="xml" indent="yes" omit-xml-declaration="no" doctype-public="-//W3C//DTD XHTML 1.1//EN" doctype-system="http://www.w3.org/TR/xhtml11/DTD/xhtml11.dtd"/>

  <xsl:template match="/">
    <html xml:lang="en-us">
      <head>
        <title>
          Job <xsl:value-of select="/Job/@name"/> {<xsl:value-of select="/Job/@id"/>} summary
        </title>
        <style type="text/css">
          <![CDATA[
          body 
          {
	          font-family: 'Segoe UI', Verdana, sans-serif;
	          font-size: small;
	          background: white;
	          color: black;
          }
          table
          {
              border-collapse: collapse;
          }
          th, td
          {
              padding: 0.2em 0.5em;
              border: solid 1px black;
          }
          th
          {
              text-align: left;
          }
          th[scope="colgroup"]
          {
            text-align: center;
          }
        ]]></style>
      </head>
      <body>
        <h1>
          Job <xsl:value-of select="/Job/@name"/> {<xsl:value-of select="/Job/@id"/>} summary
        </h1>
        <xsl:apply-templates />
      </body>
    </html>
  </xsl:template>
  <xsl:template match="JobInfo">
    <table>
      <thead>
        <tr>
          <th scope="col" rowspan="2">Start time</th>
          <th scope="col" rowspan="2">End time</th>
          <th scope="col" rowspan="2">Duration</th>
          <th scope="colgroup" colspan="5">Tasks</th>
        </tr>
        <tr>
          <th scope="col">Total</th>
          <th scope="col">Finished</th>
          <th scope="col">Errors</th>
          <th scope="col">Rack local</th>
          <th scope="col">Non data local</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            <xsl:value-of select="@startTime"/>
          </td>
          <td>
            <xsl:value-of select="@endTime"/>
          </td>
          <td>
            <xsl:value-of select="@duration"/>
          </td>
          <td>
            <xsl:value-of select="@tasks"/>
          </td>
          <td>
            <xsl:value-of select="@finishedTasks"/>
          </td>
          <td>
            <xsl:value-of select="@errors"/>
          </td>
          <td>
            <xsl:value-of select="count((Tasks | FailedTaskAttempts)[@dataDistance=1])"/>
          </td>
          <td>
            <xsl:value-of select="count((Tasks | FailedTaskAttempts)[@dataDistance=2])"/>
          </td>
        </tr>
      </tbody>
    </table>
  </xsl:template>
  <xsl:template match="Tasks | FailedTaskAttempts">
    <xsl:apply-templates select="." mode="title" />
    <table>
      <thead>
        <tr>
          <th scope="col">Task ID</th>
          <th scope="col">State</th>
          <th scope="col">Task server</th>
          <th scope="col">Attempts</th>
          <th scope="col">Start time</th>
          <th scope="col">End time</th>
          <th scope="col">Duration</th>
        </tr>
      </thead>
      <tbody>
        <xsl:apply-templates select="Task" />
      </tbody>
    </table>
  </xsl:template>
  <xsl:template match="Tasks" mode="title">
    <h2>Tasks</h2>
  </xsl:template>
  <xsl:template match="FailedTaskAttempts" mode="title">
    <h2>Failed task attempts</h2>
  </xsl:template>
  <xsl:template match="Task">
    <tr>
      <td>
        <xsl:value-of select="@id"/>
      </td>
      <td>
        <xsl:value-of select="@state"/>
      </td>
      <td>
        <xsl:value-of select="@server"/>
      </td>
      <td>
        <xsl:value-of select="@attempts"/>
      </td>
      <td>
        <xsl:value-of select="@startTime"/>
      </td>
      <td>
        <xsl:value-of select="@endTime"/>
      </td>
      <td>
        <xsl:value-of select="@duration"/>
      </td>
    </tr>
  </xsl:template>
  <xsl:template match="StageMetrics">
    <h2>Metrics</h2>
    <table>
      <thead>
        <tr>
          <th>&#160;</th>
          <xsl:apply-templates select="Stage" mode="header" />
        </tr>
      </thead>
      <tbody>
        <xsl:apply-templates select="Stage[position()=1]/Metrics/*" />
      </tbody>
    </table>
  </xsl:template>
  <xsl:template match="Stage" mode="header">
    <th scope="col">
      <xsl:value-of select="@id"/>
    </th>
  </xsl:template>
  <xsl:template match="Stage" mode="metric">
    <xsl:param name="MetricName" />
    <td>
      <xsl:value-of select="format-number(Metrics/node()[local-name()=$MetricName], '###,##0')"/>
    </td>
  </xsl:template>
  <xsl:template match="Metrics/*">
    <tr>
      <th scope="row">
        <xsl:value-of select="local-name()"/>
      </th>
      <xsl:apply-templates select="../../../Stage" mode="metric">
        <xsl:with-param name="MetricName" select="local-name()" />
      </xsl:apply-templates>
    </tr>
  </xsl:template>
</xsl:stylesheet>
