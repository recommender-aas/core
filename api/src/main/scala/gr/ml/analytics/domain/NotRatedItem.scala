package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

@ApiModel(description = "User object")
case class NotRatedItem(
                   @(ApiModelProperty @field)(value = "id of the user")
                   userId: Int,
                   @(ApiModelProperty @field)(value = "ids of not rated items")
                   itemIds: Set[Int])

object NotRatedItem