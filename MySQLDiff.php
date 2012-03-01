<?php

/**
 * Do a complete DB Diff
 */
class DB_Diff {

	protected $table_list = null;

	private $source = null;
	private $target = null;

	public function __construct(PDO $source, PDO $target, $table_list = null)
	{
		$this->source = new MySQL_DB($source);
		$this->target = new MySQL_DB($target);

		if (is_array($table_list) && !empty($table_list))
			$this->table_list = $table_list;

	}

	/**
	 * Iterate through DB and get the diff between all the tables
	 *
	 * @param boolean $execute_sql
	 * @return string
	 */
	public function getDiff($execute_sql = false)
	{
		$mods = array();
		foreach ($this->source as $table) {

			//if table doesn't exist, just copy its show create from the source DB
			if (!in_array($table, $this->target->tables)) {
				$mods[] = $this->source->handle->query("SHOW CREATE TABLE $table")->fetchColumn(1) . ";";
				continue;
			}

			$diff = new Table_Diff($this->source->handle, $this->target->handle, $table);
			$stmt = $diff->getDiff($execute_sql);
			if ($stmt) {
				$mods[] = $stmt;
			}
		}
		return implode("\n\n", $mods);
	}
}

class Table_Diff {

	protected $source;

	protected $target;

	protected $table_name;

	public function __construct(PDO $source, PDO $target, $table_name)
	{
		$this->source = $source;
		$this->target = $target;
		$this->table_name  = $table_name;

	}

	public function getDiff($execute_sql = false)
	{
		$source_table = new MySQL_Table($this->source->query("SHOW CREATE TABLE " . $this->table_name)->fetchColumn(1));
		$target_table = new MySQL_Table($this->target->query("SHOW CREATE TABLE " . $this->table_name)->fetchColumn(1));
		
		$modifications = array();

		//add in missing columns or modify columns that have different definitions
		foreach ($source_table->columns as $c_name => $c_def)
		{
			if (!array_key_exists($c_name, $target_table->columns))
				$modifications[] = sprintf("ADD COLUMN `%s` %s", $c_name, $c_def);

			elseif ($target_table->columns[$c_name] !== $c_def)
				$modifications[] = sprintf("MODIFY COLUMN `%s` %s", $c_name, $c_def);
		}

		//drop any columns that are no longer needed
		foreach ($target_table->columns as $c_name => $c_def) {
			if (!array_key_exists($c_name, $source_table->columns))
				$modifications[] = sprintf("DROP COLUMN `%s`", $c_name);
		}

		//match primary key
		if ($source_table->primary_key != $target_table->primary_key)
			$modifications[] = "DROP PRIMARY KEY, ADD PRIMARY KEY " . $source_table->primary_key;

		if (empty($modifications))
			return '';

		//build the alter table statement
		$mods = implode(",", $modifications);
		$sql  = sprintf("ALTER TABLE %s %s", $this->table_name, $mods);

		if ($execute_sql) {
			$res = $this->target->exec($sql);
		}
		
		return $sql;
	}
}

/**
 * Just parses the table's definition
 */
class MySQL_Table {
	public $columns = array();

	public $indexes = array();

	public $name = null;

	public $primary_key;

	public $uniques = array();

	public function __construct($definition)
	{
		$definition_rows = explode("\n", $definition);

		foreach ($definition_rows as $i => $_d_r) {
			if ($i == 0) {
				//get the table name
				if (!preg_match("/^CREATE\sTABLE\s`([a-zA-Z_]+)`\s\(/", $_d_r, $matches))
					throw new Exception("Unable to determine table name");

				$this->name = $matches[1];
			} else if (preg_match("/\d*`([a-zA-Z0-9_]+)`\s(.+)/", $_d_r, $matches)) {
				//get columns
				$this->columns[$matches[1]] = $this->trim($matches[2]);
			} else if (preg_match("/PRIMARY\sKEY\s(.+)/", $_d_r, $matches)) {
				$this->primary_key = $this->trim($matches[1]);
			} else if (preg_match("/(?:UNIQUE)?\sKEY\s(.+)/", $_d_r, $matches)) {
				$this->uniques[] = $this->trim($matches[1]);
			} else if (preg_match("/\)\sENGINE/", $_d_r, $matches)) {
				//skip ENGINE line
				continue;
			} else {
				throw new Exception("Unable to parse row: $_d_r on {$this->name}");
			}
		}
	}

	/**
	 * Cleanup of possible trailing commas
	 *
	 * @param string $str
	 * @return string
	 */
	private function trim($str)
	{
		if (substr($str, -1) == ',')
			$str = substr($str, 0, -1);

		return $str;
	}
}

class MySQL_DB implements Iterator {

	private $position = 0;

	public $tables = array();

	public $handle = null;

	public function __construct(PDO $handle)
	{
		$this->handle = $handle;
		$res = $this->handle->query(sprintf("select TABLE_NAME, TABLE_TYPE FROM information_schema.tables WHERE TABLE_SCHEMA = DATABASE()", true))->fetchAll(PDO::FETCH_ASSOC);
		foreach ($res as $r) {
			switch($r['TABLE_TYPE']) {
				case 'BASE TABLE':
					$this->tables[] = $r['TABLE_NAME'];
					break;
				case 'VIEW':
					$this->views[] = $r["TABLE_NAME"];
					break;
				default:
					throw new Exception("Unknown table type");
			}
		}
	}

	public function current()
	{
		if ($this->valid())
			return $this->tables[$this->position];

		throw new Exception("Invalid Table");
	}

	public function next()
	{
		$this->position++;
	}

	public function rewind()
	{
		$this->position = 0;
	}

	public function key()
	{
		return $this->position;
	}

	public function valid()
	{
		return array_key_exists($this->position, $this->tables);
	}
}
