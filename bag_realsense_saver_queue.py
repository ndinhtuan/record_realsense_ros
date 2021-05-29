import rosbag
import os
import rospy
from sensor_msgs.msg import CompressedImage, Image, CameraInfo
import threading
from datetime import datetime
from queue import Queue

class BagRealsenseSaver(object):

    def __init__(self, top_dir_saving, using_compressed_topic=False):
        
        self.using_compressed_topic = using_compressed_topic

        if not os.path.exists(top_dir_saving):
            os.makedirs(top_dir_saving)

        self.top_dir_saving = top_dir_saving

        if not using_compressed_topic:
            self.color_topic = "/camera/color/image_raw"
        else:
            self.color_topic = "/camera/color/image_raw/compressed"

        self.depth_topic = "/camera/aligned_depth_to_color/image_raw"
        self.info_topic = "/camera/depth/camera_info"

        self.init_topic()
        name_file = self.get_folder_hourly_log()
        self.bag_file = rosbag.Bag("{}/{}.bag".format(top_dir_saving, name_file), 'w')

        self.sema_color = threading.Semaphore()
        self.sema_depth = threading.Semaphore()
        self.sema_info = threading.Semaphore()
    
        self.max_size=10
        self.color_data = Queue(maxsize=self.max_size) #None
        self.depth_data = Queue(maxsize=self.max_size) #None
        self.info_data = Queue(maxsize=self.max_size) #None
        
        self.prev_seq_color = -1
        self.prev_seq_depth = -1
        self.prev_seq_info = -1

        self.cur_seq_color = -1
        self.cur_seq_depth = -1
        self.cur_seq_info = -1
    
    def get_folder_hourly_log(self, duration_hour=1):

        proposed_hourly_log = [i for i in range(0, 24, duration_hour)]
        date = datetime.now()
        curr_hour = int(date.strftime("%H"))
        choosed_index = int(curr_hour/duration_hour)
        return proposed_hourly_log[choosed_index]
    
    def init_topic(self):
        
        if not self.using_compressed_topic:
            rospy.Subscriber(self.color_topic, Image, self.color_callback)
        else:
            rospy.Subscriber(self.color_topic, CompressedImage, self.color_callback_compressed)

        rospy.Subscriber(self.depth_topic, Image, self.depth_callback)
        rospy.Subscriber(self.info_topic, CameraInfo, self.intrin_callback)
    
    def color_callback(self, data):
        
        self.sema_color.acquire()
        #self.color_data = data
        try:
            self.color_data.put(data, False)
        except Exception:
            self.sema_color.release()
        #print(data.header)
        self.cur_seq_color = data.header.seq
        self.sema_color.release()

    def color_callback_compressed(self, data):
        
        self.sema_color.acquire()
        #self.color_data = data

        try:
            self.color_data.put(data, False)
        except Exception:
            self.sema_color.release()

        self.cur_seq_color = data.header.seq
        self.sema_color.release()

    def depth_callback(self, data):
            
        self.sema_depth.acquire()
        #self.depth_data = data
        try:
            self.depth_data.put(data, False)
        except Exception:
            self.sema_depth.release()
        self.cur_seq_depth = data.header.seq
        self.sema_depth.release()

    def intrin_callback(self, data):
        
        self.sema_info.acquire()
        #self.info_data = data
        try:
            self.info_data.put(data, False)
        except Exception:
            self.sema_info.release()

        self.cur_seq_info = data.header.seq
        self.sema_info.release()

    def hourly_saving(self):
        
        #self.sema_color.acquire()
        #self.sema_depth.acquire()
        #self.sema_info.acquire()
        
        name_file = self.get_folder_hourly_log()
        path_bag_file = "{}/{}.bag".format(self.top_dir_saving, name_file)

        if not os.path.isfile(path_bag_file):
            self.bag_file.close()
            self.bag_file = rosbag.Bag(path_bag_file, 'w')
        else:

            if self.color_data is not None and self.cur_seq_color != self.prev_seq_color: 
                print("Saving color data")
                self.sema_color.acquire()
                try:
                    color_data = self.color_data.get(False)
                    self.bag_file.write(self.color_topic, color_data)
                    self.prev_seq_color = self.cur_seq_color

                except Exception:
                    self.sema_color_release()

                self.sema_color.release()
            if self.depth_data is not None and self.cur_seq_depth != self.prev_seq_depth:
                print("Saving depth data")
                self.sema_depth.acquire()
                try:
                    depth_data = self.depth_data.get(False)
                    self.bag_file.write(self.depth_topic, depth_data)
                    self.prev_seq_depth = self.cur_seq_depth
                except Exception:
                    self.sema_depth.release()

                self.sema_depth.release()
            if self.info_data is not None and self.cur_seq_info != self.prev_seq_info:
                print("Saving info data")
                self.sema_info.acquire()
                try :
                    info_data = self.info_data.get(False)
                    self.bag_file.write(self.info_topic, info_data)
                    self.prev_seq_info = self.cur_seq_info
                except Exception:
                    self.sema_info.release()
                self.sema_info.release()

        #print("Saved at {}".format(path_bag_file))

        #self.sema_info.release()
        #self.sema_depth.release()
        #self.sema_color.release()

    def get_bag_file(self):
        return self.bag_file

if __name__=="__main__":
    
    bag_saver = BagRealsenseSaver("test")
    rospy.init_node("bag_saver_node", anonymous=True)
    
    while True:
        try:
            bag_saver.hourly_saving()
        except Exception:
            bag_saver.get_bag_file().close()
            exit()

    rospy.spin()
