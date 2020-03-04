import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e4e3e6bc16cffd76fc72c76','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	MovieRecommendation_Demo_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e4e3e6bc16cffd76fc72c76", spark, "{'url': '/Demo/MovieRatingsTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapi2936395fb3cbcb995e4fe803cc653542', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e4e3e6bc16cffd76fc72c76','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e4e3e6bc16cffd76fc72c76','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e4e3e6bc16cffd76fc72c77','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	MovieRecommendation_Demo_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e4e3e6bc16cffd76fc72c76"],{"5e4e3e6bc16cffd76fc72c76": MovieRecommendation_Demo_DBFS}, "5e4e3e6bc16cffd76fc72c77", spark,json.dumps( {"FE": [{"feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "25000", "mean": "462.83", "stddev": "267.09", "min": "1", "max": "943", "missing": "0"}}, {"feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "25000", "mean": "425.34", "stddev": "330.71", "min": "1", "max": "1682", "missing": "0"}}, {"feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "25000", "mean": "3.53", "stddev": "1.12", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "25000", "mean": "3.53", "stddev": "0.46", "min": "1.51", "max": "4.94", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "16.43", "stddev": "8.29", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "6.67", "stddev": "4.35", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "type": "numeric", "selected": "True", "stats": {"count": "1000", "mean": "1997.48", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e4e3e6bc16cffd76fc72c77','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e4e3e6bc16cffd76fc72c77','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e4e3e6bc16cffd76fc72c78','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
	MovieRecommendation_Demo_AutoML = tpot_execution.Tpot_execution.run(["5e4e3e6bc16cffd76fc72c77"],{"5e4e3e6bc16cffd76fc72c77": MovieRecommendation_Demo_AutoFE}, "5e4e3e6bc16cffd76fc72c78", spark,json.dumps( {"model_type": "classification", "label": "Rating", "features": ["UserId", "MovieId", "Timestamp", "AvgRating", "Timestamp_dayofmonth", "Timestamp_month", "Timestamp_year"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "none", "run_id": "32d380e0aa73406c8165dd557b0ea6b8", "model_id": "5e5f7e9777f12376d1ca3284", "ProjectName": "Retail Scenarios", "PipelineName": "MovieRecommendationDemo", "pipelineId": "5e4e3e6bc16cffd76fc72c75", "userid": "5e58ebb7957f3f13254389b5", "runid": "", "url_ResultView": "http://23.99.85.149:3200", "experiment_id": "2341748169460103"}))

	PipelineNotification.PipelineNotification().completed_notification('5e4e3e6bc16cffd76fc72c78','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e4e3e6bc16cffd76fc72c78','5e58ebb7957f3f13254389b5','http://23.99.85.149:3200/pipeline/notify','http://23.99.85.149:3200/logs/getProductLogs')
	sys.exit(1)

