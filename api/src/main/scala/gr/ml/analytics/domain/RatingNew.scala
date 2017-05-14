package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

@ApiModel(description = "gr.ml.analytics.domain.gr.ml.analytics.domain.Rating object")
case class RatingNew(
                   @(ApiModelProperty @field)(value = "unique identifier for the user")
                   userId: Int,
                   @(ApiModelProperty @field)(value = "unique identifier for the item")
                   itemId: Int,
                   @(ApiModelProperty @field)(value = "rating to describe how user prefer an item")
                   rating: Double,
                   @(ApiModelProperty @field)(value = "feature values of the item")
                   features: List[Double])

object RatingNew