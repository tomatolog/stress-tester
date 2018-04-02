<?php
class plugin {

	public function init() {
		$options = getopt('', array('url::', 'rawtime', 'logreply'));
		$url = isset($options['url']) ? $options['url'] : 'http://127.0.0.1:8380/json/pq/pq/_search';
		$this->rawtime = isset($options['rawtime']);
		$this->logreply = isset($options['logreply']);
		$this->batch = 0;
		
		$this->ci = curl_init();
        curl_setopt($this->ci, CURLOPT_URL, $url);
		curl_setopt($this->ci, CURLOPT_CUSTOMREQUEST, 'POST');
		curl_setopt($this->ci, CURLOPT_HTTPHEADER, array('Expect:', 'Content-Type: application/json'));
		curl_setopt($this->ci, CURLOPT_RETURNTRANSFER, 1);
		//curl_setopt($this->ci, CURLOPT_VERBOSE, '1'); 
	}

	public function count_hits($res) {
		$queries = array();
		foreach ($res->hits->hits as $doc) {
			foreach ($doc->matches as $q) {
				if (isset($queries[$q])) {
					array_push($queries[$q], $doc->doc);
				} else {
					$queries[$q] = array($doc->doc);
				}
			}
		}
		
		if ($this->logreply && count($queries)>0) {
			ksort($queries);
			echo "\nbatch=$this->batch";
			foreach ($queries as $q=>$d) {
				sort($d);
				echo "\n\tq=$q: " . implode(",", $d);
			}
		}
		return count($queries);
	}
	
	public function print_hits($res, $doc_ids) {
		if (count($res->hits->hits)==0) {
			return;
		}
			
		echo "\nbatch=$this->batch";
		foreach ($res->hits->hits as $q) {
			$docs = array();
			foreach($q->fields->_percolator_document_slot as $d) {
				array_push($docs, $doc_ids[$d-1]);
			}
			//echo "\n\tq=$q->_id: " . implode(",", array_values($docs));
			$qraw = $q->_source->query->ql;
			echo "\n\tq=$q->_id: " . implode(",", array_values($docs)) . " $qraw";
		}
	}
	
	public function query($docs) {
		
		$out = array();
		reset($docs);
		$id = key($docs);
		$json = array("id" =>$id, "query" => array("percolate" => array()));
		$doc_ids = array();
		if (count($docs)>1) {
			$d = array();
			foreach ($docs as $id => $v) {
				array_push($d, array("field"=>$v));
				array_push($doc_ids, $id);
			}
			$json["query"]["percolate"]["documents"] = $d;
		} else {
			array_push($doc_ids, $id);
			$json["query"]["percolate"]["document"] = array("field" => reset($docs));
		}
		$json = json_encode($json);
		//var_dump($json);

		curl_setopt($this->ci, CURLOPT_POSTFIELDS, $json);
		$tm_start = microtime(true);
		$res = curl_exec($this->ci);
		$tm = microtime(true) - $tm_start;
		$res = json_decode($res);
		//var_dump($res);
		
		$rows = count($res->hits->hits);
		if (isset($res->took_query_build)) {	// Luwak case to count and print docs per query
			$rows = $this->count_hits($res);
		} else if ($this->logreply) {			// Manticore case to print docs per query
			$this->print_hits($res, $doc_ids);
		}
		
		if ($this->rawtime) {
			$tm = 0.001 * $res->took;
		}
		$httpcode = curl_getinfo($this->ci, CURLINFO_HTTP_CODE);
		$errors = 0;
		if ( $httpcode!=200 ) {
			$errors = count($docs);
		}
		//var_dump($tm, $res->took);
		
		foreach ($docs as $id => $v) {
			$out[$id] = array('latency' => $tm, 'hits' => 0, 'errors' => 0, 'requests' => 0);
		}
		$last = array_pop($out);
		$last['hits'] = $rows;
		$last['errors'] = $errors;
		$last['requests'] = 1;
		array_push($out, $last);
		$this->batch++;
		
		return $out;
	}
	

	public static function report($queriesInfo) {
		$totalMatches = 0;
		$errors = 0;
		$requests = 0;
		foreach($queriesInfo as $id => $info) {
			$totalMatches += $info['hits'];
			$errors += $info['errors'];
			$requests += $info['requests'];
		}
		return array(
		'Total docs' => $totalMatches,
		'Total requests' => $requests,
		'Errors' => $errors);
	}
}
