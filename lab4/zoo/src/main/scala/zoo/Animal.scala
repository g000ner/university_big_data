package zoo

import org.apache.zookeeper._

case class Animal(name: String, hostPort: String, root: String, partySize: Integer) extends Watcher {
  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val animalPath = root + "/" + name

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      println(s"Event from keeper: ${event.getType}")
    }
  }

  def enter(): Boolean = {
    zk.create(animalPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)

    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for the others.")
          mutex.wait()
          println("Noticed someone.")
        } else {
          return true
        }
      }
    }
    return false
  }

  def leave(): Unit = {
    zk.delete(animalPath, -1)
  }
}