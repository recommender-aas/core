package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

@ApiModel(description = "User object")
case class NotRatedItemWithFeatures(
                   @(ApiModelProperty @field)(value = "id of the user")
                   userId: Int,
                   @(ApiModelProperty @field)(value = "ids of not rated items with their features")
                   items: Map[Int, List[Double]])

object NotRatedItemWithFeatures