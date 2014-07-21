
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
    # TODO(tarnfeld): Resolve to an IP address as hostnames cause issues
    # with libprocess PIDs
    self.leader = leader

  def appoint(self, leader):
    pass

  def detect(self, previous=None):
    return self.leader
