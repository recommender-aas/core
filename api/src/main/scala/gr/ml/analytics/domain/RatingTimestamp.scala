package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

@ApiModel(description = "gr.ml.analytics.domain.Rating object")
case class RatingTimestamp(
                   @(ApiModelProperty @field)(value = "year of rating - serving as partition key")
                   year: Int,
                   @(ApiModelProperty @field)(value = "unique identifier for the user")
                   userId: Int,
                   @(ApiModelProperty @field)(value = "int timestamp when user rated item (in seconds)")
                   timestamp: Int)

object RatingTimestamp