package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

@ApiModel(description = "gr.ml.analytics.domain.gr.ml.analytics.domain.User object")
case class User(
                   @(ApiModelProperty @field)(value = "id of the user")
                   userId: Int)

object User