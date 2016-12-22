<?php
/**
 * PHP Zookeeper
 *
 * PHP Version 5.3
 *
 * The PHP License, version 3.01
 *
 * @category  Libraries
 * @package   PHP-Zookeeper
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2012 PHP Group
 * @license   http://www.php.net/license The PHP License, version 3.01
 * @link      https://github.com/andreiz/php-zookeeper
 */

/**
 * Example interaction with the PHP Zookeeper extension
 *
 * @category  Libraries
 * @package   PHP-Zookeeper
 * @author    Lorenzo Alberton <l.alberton@quipo.it>
 * @copyright 2012 PHP Group
 * @license   http://www.php.net/license The PHP License, version 3.01
 * @link      https://github.com/andreiz/php-zookeeper
 */
class MyZookeeper
{
	/**
	 * @var Zookeeper
	 */
	private $zookeeper;

	/**
	 * Constructor
	 *
	 * @param string $address CSV list of host:port values (e.g. "host1:2181,host2:2181")
	 */
	public function __construct($address) {
		$this->zookeeper = new Zookeeper($address);
	}

  public function exists($path) {
    return $this->zookeeper->exists($path);
  }

  public function delete($path) {
    return $this->zookeeper->delete($path);
  }

	/**
	 * Set a node to a value. If the node doesn't exist yet, it is created.
	 * Existing values of the node are overwritten
	 *
	 * @param string $path  The path to the node
	 * @param mixed  $value The new value for the node
	 *
	 * @return mixed previous value if set, or null
	 */
	public function set($path, $value, $params, $flags) {
		if (!$this->zookeeper->exists($path)) {
			$this->makePath($path);
			return $this->makeNode($path, $value, $params, $flags);
		} else {
			return $this->zookeeper->set($path, $value);
		}
	}

  // 创建节点，如果节点已经存在，则返回null
  public function create($path, $value, $params = array(), $flags = null) {
		if (!$this->zookeeper->exists($path)) {
			$this->makePath($path);
			return $this->makeNode($path, $value, $params, $flags);
		} else {
			return null;
		}
  }

	/**
	 * Equivalent of "mkdir -p" on ZooKeeper
	 *
	 * @param string $path  The path to the node
	 * @param string $value The value to assign to each new node along the path
	 *
	 * @return bool
	 */
	public function makePath($path, $value = '', $params = array(), $flags = null) {
		$parts = explode('/', $path);
    array_shift($parts); //去掉开始的空白
		//$parts = array_filter($parts);
		$subpath = '';
		while (count($parts) > 1) {
			$subpath .= '/' . array_shift($parts);
			if (!$this->zookeeper->exists($subpath)) {
				$this->makeNode($subpath, $value);
			}
		}
	}

	/**
	 * Create a node on ZooKeeper at the given path
	 *
	 * @param string $path   The path to the node
	 * @param string $value  The value to assign to the new node
	 * @param array  $params Optional parameters for the Zookeeper node.
	 *                       By default, a public node is created
	 *
	 * @return string the path to the newly created node or null on failure
	 */
	public function makeNode($path, $value, array $params = array(), $flags = null) {
		if (empty($params)) {
			$params = array(
				array(
					'perms'  => Zookeeper::PERM_ALL,
					'scheme' => 'world',
					'id'     => 'anyone',
				)
			);
		}
		return $this->zookeeper->create($path, $value, $params, $flags);
	}

	/**
	 * Get the value for the node
	 *
	 * @param string $path the path to the node
	 *
	 * @return string|null
	 */
	public function get($path) {
		if (!$this->zookeeper->exists($path)) {
			return null;
		}
		return $this->zookeeper->get($path);
	}

	/**
	 * List the children of the given path, i.e. the name of the directories
	 * within the current node, if any
	 *
	 * @param string $path the path to the node
	 *
	 * @return array the subpaths within the given node
	 */
	public function getChildren($path) {
		if (self::exists($path)){
		    if (strlen($path) > 1 && preg_match('@/$@', $path)) {
		        // remove trailing /
		        $path = substr($path, 0, -1);
		    }
		    return $this->zookeeper->getChildren($path);
		}else{
		    return false;
		}
	    
	}
}
