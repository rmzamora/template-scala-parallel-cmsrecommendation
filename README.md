CMS Recommendation Template
===========================


###Overview

This engine template is based on [SCALA e-commerce recommendion template](http://templates.prediction.io/PredictionIO/template-scala-parallel-ecommercerecommendation) provides personalized recommendation for CMS applications with the following features by default:

Recommend unseen articles only (configurable)
Recommend popular articles if no information about the user is available

###Usage

#Event Data Requirements
By default, this template takes the following data from Event Server:

* Users' view events
* Users' like events
* Users' share events
* Users' rate events
* Articles' with categories and tag properties


Sample training data is not included. However a training data is available for the Mosaic&trade; sandbox. for more information about the platform visit [movent.com](http://www.movent.com)

###Input Query
* UserID
* Num of articles to be recommended
* List of white-listed article categories and/or tags (optional)

The template also supports black-list and whitelist. If a whitelist is provided, the engine will include only those articles in the recommendation. Likewise, if a blacklist is provided, the engine will exclude those articles in the recommendation.

###Output PredictedResult
A ranked list of recommended articleIds

###For full documentation please refer to [ecommercerecommendation](https://docs.prediction.io/templates/ecommercerecommendation/quickstart/) where this engine is based on.

###Changes on installation

1). Create a new Engine from an Engine Template based from the document but replace the engine source to 'rmzamora/template-scala-parallel-cmsrecommendation'

2).Changes to engine.json.

```
"algorithms": [
 {
   "name": "cms",
   "params": {
     "appName": "YOUR_APP_NAME",
     "unseenOnly": false,
     "seenEvents": ["like", "view", "share", "rate"],
     "similarEvents": ["view"],
     "rank": 10,
     "numIterations" : 10,
     "lambda": 0.01,
     "seed": 3
   }
 }
]
```



