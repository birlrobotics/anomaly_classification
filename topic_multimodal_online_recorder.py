from multiprocessing import Queue
import multiprocessing
import birl.HMM.hmm_for_baxter_using_only_success_trials.hmm_online_service.data_stream_handler_process as data_stream_handler_process
import birl.HMM.hmm_for_baxter_using_only_success_trials.hmm_online_service.constant as constant 
import birl.robot_introspection_pkg.multi_modal_config as mmc
import rospy
from anomaly_classification_proxy.srv import (
    AnomalyClassificationService, 
    AnomalyClassificationServiceResponse,
)

class RedisTalker(multiprocessing.Process):
    def __init__(
        self,
        com_queue, 
    ):
        multiprocessing.Process.__init__(self)     
        self.com_queue = com_queue
        
    def run(self):
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        print 'delete key \"tag_multimodal_msgs\"', r.delete("tag_multimodal_msgs")
        while True:
            try:
                latest_data_tuple = self.com_queue.get(1)
            except multiprocessing.Queue.Empty:
                continue

            data_frame = latest_data_tuple[constant.data_frame_idx]
            smach_state = latest_data_tuple[constant.smach_state_idx]
            data_header = latest_data_tuple[constant.data_header_idx]

            score = data_header.stamp.to_sec()
            value = data_frame
            r.zadd("tag_multimodal_msgs", value, score)

if __name__ == '__main__':
    com_queue_of_receiver = Queue()
    process_receiver = data_stream_handler_process.TagMultimodalTopicHandler(
        mmc.interested_data_fields,
        com_queue_of_receiver,
        node_name="tagMsgReceiverForOnlineRedisRecorder",
    )
    process_receiver.start()

    com_queue_of_redis = Queue()
    redis_talker = RedisTalker(com_queue_of_redis)
    redis_talker.start()

    rospy.init_node('anomaly_classification_node')
    def tmp_callback(req):
        print req
        return AnomalyClassificationServiceResponse(1, 0.99)
    s = rospy.Service("AnomalyClassificationService", AnomalyClassificationService, tmp_callback) 

    while not rospy.is_shutdown():
        try:
            latest_data_tuple = com_queue_of_receiver.get(1)
        except Queue.Empty:
            continue
        latest_data_tuple = com_queue_of_receiver.get()
        com_queue_of_redis.put(latest_data_tuple)

    process_receiver.shutdown()
    redis_talker.shutdown()
