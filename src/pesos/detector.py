class MasterDetector(object):
  @classmethod
  def from_url(cls, url):
    pass

  def detect(self, previous=None):
    pass


class StandaloneMasterDetector(MasterDetector):
  @classmethod
  def master_info_from_pid(cls, pid):
    pass

  def __init__(self, leader):
    self.leader = leader

  def appoint(self, leader):
    pass

  def detect(self, previous=None):
    pass
