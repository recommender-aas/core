**Recommender as a Service** is a content-agnostic recommendation SaaS that provides real-time item recommendations for specific user based on 
his rating history.

System receives user ratings via REST API from customer's web-sites/mobile clients and stores it as input for background machine learning jobs.

**[Link to REST API](http://172.104.136.67:18080/swagger)**

2 separate pipelines are used for making predictions: 

1. **Content-based** (based on item feature values)
2. **Collaborative** (based on ratings from other users)

Both pipelines converge into final **Hybrid** pipeline that combines predictions from the previous two:

![Hybrid Recommender System](https://github.com/GRpro/recommender_as_a_service/blob/master/images/Recommender_systems.png)

Every dataset is different from another and requires tailor-made tuning in order to achieve best performance. For this purpose the system automatically chooses algorithms, weights and parameters in order to achieve the best performance for each dataset.

Before returning final predictions the system combines results of Hybrid recommendations with most popular and the newest items. It helps to avoid poor quality predictions for new users and items:

<img src="https://github.com/GRpro/recommender_as_a_service/blob/master/images/Hybrid_model.png" width="500">


