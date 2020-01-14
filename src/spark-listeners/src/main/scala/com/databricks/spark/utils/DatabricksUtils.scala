package com.databricks.spark.utils

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.gson.Gson
import org.apache.http.impl.client.{BasicResponseHandler, HttpClientBuilder}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext, TaskContext}

case class Email(`type`: String, value: String, primary: Boolean)
case class Name(familyName: String, givenName: String)
case class UserInfo(emails: Array[Email] = null,
                    displayName: String = null,
                    schemas: Array[String] = null,
                    name: Name = null,
                    active: Boolean = false,
                    groups: Array[String] = null,
                    id: String = null,
                    userName: String = null)
case class KeyValue(key: String = null, value: String = null)

object DatabricksUtils {
  private val DATABRICKS_API_URL_PROPERTY = "spark.databricks.api.url"
  private val DATABRICKS_API_TOKEN_PROPERTY = "spark.databricks.token"
  private val DATABRICKS_USER_PROFILE_ENDPOINT = "/api/2.0/preview/scim/v2/Me"
  private val CACHE: LoadingCache[String, UserInfo] = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .expireAfterWrite(60, TimeUnit.MINUTES)
    .build(new CacheLoader[String, UserInfo]() {
      override def load(key: String): UserInfo = {
        val Array(domain, token) = key.split(":::")
        getUserInfo(domain, token)
      }
    })

  private def getUrlFromWorker(): String = {
    try {
      TaskContext
        .get()
        .getLocalProperty(DATABRICKS_API_URL_PROPERTY)
    } catch {
      case ex: Exception => {
        SparkSession.getDefaultSession.get.sparkContext
          .getLocalProperty(DATABRICKS_API_URL_PROPERTY)
      }
    }
  }

  private def getTokenFromWorker(): String = {
    try {
      TaskContext
        .get()
        .getLocalProperty(DATABRICKS_API_TOKEN_PROPERTY)
    } catch {
      case ex: Exception => {
        SparkSession.getDefaultSession.get.sparkContext
          .getLocalProperty(DATABRICKS_API_TOKEN_PROPERTY)
      }
    }
  }

  private def getUrlFromDriver(sparkContext: SparkContext): String = {
    sparkContext.getLocalProperty(DATABRICKS_API_URL_PROPERTY)
  }

  private def getTokenFromDriver(sparkContext: SparkContext): String = {
    sparkContext.getLocalProperty(DATABRICKS_API_TOKEN_PROPERTY)
  }

  def getDBToken(): String = {
    SparkSession.getActiveSession.fold(getTokenFromWorker())(
      s => getTokenFromDriver(s.sparkContext)
    )
  }

  def getDomain(): String = {
    SparkSession.getActiveSession.fold(getUrlFromWorker())(
      s => getUrlFromDriver(s.sparkContext)
    )
  }

  def getUserName(): UserInfo = {
    val domain = getDomain()
    val token = getDBToken()
    val resp = CACHE.get(s"$domain:::$token")
    resp
  }


  def getUserInfo(domain: String, token: String): UserInfo = {
    val url = s"$domain$DATABRICKS_USER_PROFILE_ENDPOINT"
    val client = HttpClientBuilder.create().build()
    val request = new org.apache.http.client.methods.HttpGet(url)
    request.addHeader("Authorization", s"Bearer $token")
    val response = client.execute(request)
    val handler = new BasicResponseHandler()
    new Gson()
      .fromJson(handler.handleResponse(response).trim, classOf[UserInfo])
  }

  def getUserInfo(response: String): UserInfo = {
    new Gson().fromJson(response, UserInfo.getClass)
  }

}

