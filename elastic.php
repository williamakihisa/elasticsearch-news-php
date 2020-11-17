<?php
if (!defined('BASEPATH')) exit('No direct script access allowed');
header('Access-Control-Allow-Origin: *');
header("Access-Control-Allow-Methods: *");
class Elastic extends CI_Controller {
	function __construct() {
    parent::__construct();
		$valid_passwords = array (
					"anyuser" => "anypassword",
				);
				$valid_users = array_keys($valid_passwords);

				$user = $_SERVER['PHP_AUTH_USER'];
				$pass = $_SERVER['PHP_AUTH_PW'];

				$validated = (in_array($user, $valid_users)) && ($pass == $valid_passwords[$user]);

				if (!$validated) {
					header('WWW-Authenticate: Basic realm="CMS"');
					header('HTTP/1.0 401 Unauthorized');
					die ("Not authorized");
				}
		$this->load->library('elasticsearch_new');
		$this->load->library('elasticsearch_new2');
	}
	private $keyAccess = array(
		"anyuser"=>"anykey"
	);
	protected $allowed_type = array('text', 'date', 'integer', 'double', 'geo', 'keyword');
	protected $articles_field = array('id', 'title', 'subtitle', 'tag', 'keyword', 'alias', 'alias_old', 'frontpage_section', 'frontpage_category', 'section_id', 'category_id', 'youtube', 'introtext',
																		'fulltexts', 'written_date', 'publish_date', 'publish', 'written_by', 'editor_by', 'source', 'foto_type', 'livereport', 'wiki_blog', 'foto_name', 'foto_source',
																	  'foto_caption', 'related_id', 'writter_fullname', 'writter', 'writter_id', 'editor_fullname', 'editor', 'editor_id', 'hit', 'publish', 'editor_video_by', 'section');
	protected $mandatory_articles = array('id', 'title', 'fulltext');
	protected $mandatory_search = array('table', 'limit');

	private $authkey = '3L4st1CM@5t3R';

	function create_table(){
			$req = $this->input->post();
			if (!$req){
				 $req = json_decode(file_get_contents('php://input'), true);
			}
			$check = $this->validate($req, 'create');
			if (count($check) == 0){
				 $response = json_encode($this->elasticsearch_new->create($req, ''));
			}else{
				$response = implode(", ", $check);
			}
		 echo $response;
	}

	function insert(){
		$req = $this->input->post();
		$tmp = json_decode(json_encode($req), true);

		// foreach ($tmp as $tmpval) {
		// 	 $req = $tmpval;
		// }
		$req = json_decode($req, true);

		if (!$req){
			 $string = file_get_contents('php://input');
			 $req = json_decode($string, true);
		}

		if (isset($req['row'])){
			$req['row'] = str_replace(array("\r", "\n"), '', $req['row']);
			$req['row'] = json_decode($req['row'], true);
		}

		$check = $this->validate($req, 'insert');
		if (count($check) == 0){
			log_message('ERROR','check insert data row 1 >>>>>>>> '.json_encode($req['row']));
			$response = json_encode($this->elasticsearch_new->insert($req['table'], $req['row'], ''));
		}else{
			if (strpos($check[0], 'fielddiff=') !== false){
				 $newkeys = str_replace("fielddiff=","",$check[0]);
				 $newkeys = explode(",",$newkeys);
				 for ($i=0; $i < count($req['row']); $i++) {
				 	  foreach ($newkeys as $keys) {
							if (!(isset($req['row'][$i][$keys]))){
								 $req['row'][$i][$keys] = '';
							}
				 	  }
				 }
				 log_message('ERROR','check insert data row 2 >>>>>>>> '.json_encode($req['row']));
				 $response = json_encode($this->elasticsearch_new->insert($req['table'], $req['row'], ''));
			}else{
				log_message('ERROR','check insert data row 3 >>>>>>>> '.json_encode($check));
				$response = implode(", ", $check);
			}
		}
		echo $response;
	}

	function search(){
 		$req = $this->input->get();
 		$check = $this->validate($req, 'search');
 		if (count($check) == 0){

 			$reqkey = $newReq = $findReq = $sort = array();
 			if ((isset($req['field']) && strlen($req['field']) != 0) && (isset($req['keyword']) && strlen($req['keyword']) != 0)){
 				$reqkey = explode(",",$req['field']);

 				if ((strpos($req['keyword'], '-') !== false) || (strpos($req['field'], 'id') !== false)) {
					if (strpos($req['field'], 'date') !== false){
						$newReq[] = array(
							$reqkey[0] => 'date'
						);
					}else{
						$newReq[] = array(
							$req['field'] => 'strict'
						);
					}
 				}else{
 					if (strpos($req['field'], 'date') !== false){
 						$newReq[] = array(
 							$reqkey[0] => 'date'
 						);
 					}else{
						if (strpos($req['keyword'], '|') !== false){
							$newReq[] = array(
								$req['field'] => 'or'
							);

						}else{
							$newReq[] = array(
								json_encode($reqkey) => 'combine_wildcard'
							);
						}

 					}
 				}
				if (strpos($req['keyword'], '|') !== false){
					$findReq[] = str_replace('|',' or ',$req['keyword']);
				}else{
					$findReq[] = str_replace('%20',' ',$req['keyword']);
				}

 			}
			if (isset($req['ignore']) && isset($req['ignore_key'])){
				 $ignorefield = explode(",",$req['ignore']);
				 foreach ($ignorefield as $keyign) {
				 	 $newReq[] = array(
						 $req['ignore_key'] => 'ignore'
					 );
					 $findReq[] = str_replace('%20',' ',$keyign);
				 }
			}
 			$limit = (isset($req['limit'])) ? $req['limit'] : 10;

 			if (isset($req['sort_by']) && isset($req['sort_type'])){
 				 $sort = array('field' => $req['sort_by'], 'type' => $req['sort_type']);
 			}

			if (isset($req['range']) && isset($req['range_date'])){
				$newReq[] = array(
					$req['range'] => 'range'
				);
				$findReq[] = str_replace('%20',' ',$req['range_date']);
			}

			$excl = '';
			if (isset($req['exclude'])){
				 $excl = $req['exclude'];
			}
 			$page = (isset($req['page'])) ? $req['page'] : 0;
 			log_message('ERROR','check elastic tes 1 >>>'.json_encode($newReq).' --- '.json_encode($findReq));

 			$response = json_encode($this->elasticsearch_new->search($req['table'], $newReq, $findReq, $sort, $page, $limit, 0, '', $excl));
 		}else{
 			$response = implode(", ", $check);
 		}
 		echo $response;
 	}


	function delete(){
		$req = $this->input->get();
		$check = $this->validate($req, 'delete');
		if (count($check) == 0){
			 $ids = explode(",",$req['id']);
			 $successdel = 0;$faildel = array();
			 foreach ($ids as $id) {
			 	  $statusdelete = $this->elasticsearch_new->delete($req['table'], $id, '');
					if ($statusdelete['status'] == 1){
							$successdel++;
					}else{
						$faildel[] = $id;
					}
			 }
			 if ($successdel == count($ids)){
				 $response = json_encode(array('status' => 1, 'message' => 'success'));
			 }else{
				 $response = 'id failed to delete : '.implode(",",$faildel);
			 }
		}else{
			$response = implode(", ", $check);
		}
		echo $response;
	}
  function update(){
		$req = json_decode($this->input->post(), true);

		if (!$req){
			 $req = json_decode(file_get_contents('php://input'), true);
		}
		// if (isset($req['data'])){
		// 	$req['data'] = str_replace(array("\r", "\n"), '', $req['data']);
		// 	$req['data'] = json_decode($req['data'], true);
		// }


		$check = $this->validate($req, 'update');
		if (count($check) == 0){
			$response = json_encode($this->elasticsearch_new->update($req['table'], $req['id'], $req['data'], ''));
		}else{
			if (strpos($check[0], 'fielddiff=') !== false){
				 $response = json_encode($this->elasticsearch_new->update($req['table'], $req['id'], $req['data'], ''));
			}else{
				$response = implode(", ", $check);
			}
		}
		echo $response;
	}
  private function validate($post = array(), $method = ''){
		 $message = array();
		 if ($method == 'create'){
			  foreach ($post as $key => $elmt) {
			  	 if (count($elmt) > 0){
						 foreach ($elmt as $keyelmt => $valueelmt) {
						 	if (!(in_array($valueelmt, $this->allowed_type))) $message[] = $valueelmt.' type are not allowed!';
						 }
					 }else{
						 $message[] = $key.' should have one or more fields!';
					 }
			  }
		 }
		 if ($method == 'insert'){
			  if (isset($post['table']) && isset($post['row']) && count($post['row']) > 0){
					 if ($post['table'] == 'articles'){
						  for ($i=0; $i < count($post['row']); $i++) {
								 $fields = $emptyData = array();$emptyField = 0;
								 foreach ($this->articles_field as $field) {
									 foreach ($post['row'][$i] as $key => $value) {
											 foreach ($this->mandatory_articles as $mandatkey) {
												 if ($key == $mandatkey){
													 if (strlen(trim($value)) == 0){
														 $emptyField = 1;
														 $emptyData[] = $key;
														 break;
													 }
												 }
											 }
											if ($field == $key) $fields[] = $key;
									 }
								 }
								 if ($emptyField == 1){
									 $message[] = 'fields '.implode(',',$emptyData).' in row '.$i.' cannot empty!';
								 }else{
									 if (count($fields) != count($this->articles_field)){
										  $diffKey = array_diff_key( $this->articles_field,$fields);
											$newkeys = array();
											foreach ($diffKey as $keydif => $valuedif) {
												 $newkeys[] = $valuedif;
											}
											$message[] = 'fielddiff='.implode(',',$newkeys);
									 }
								 }
						  }
					 }
				}else{
					$message[] = 'table or row not exist!';
				}
		 }
		 if ($method == 'search'){
			  if (!isset($post['table'])) $message[] = 'table not exists!';
			  foreach ($this->mandatory_search as $key) {
					  $exists = 0;
			  	  foreach ($post as $postkey => $value) {
							 if ($postkey == $key) $exists = 1;
			  	  }
						if ($exists == 0) $message[] = $key.' not exists!';
			  }
		 }
		 if ($method == 'delete'){
			 if (!isset($post['table'])) $message[] = 'table not exists!';
				 if (isset($post['id'])){
					  $check = explode(",", $post['id']);
						if (count($check) > 0){
							foreach ($check as $ids) {
								if (strlen(trim($ids)) == 0) $message[] = 'id empty or not valid!';
							}
						}else{
							$message[] = 'id cannot empty!';
						}
				 }else{
					 $message[] = 'id not exists!';
				 }
		 }
		 if ($method == 'update'){
			  if (!isset($post['table'])) $message[] = 'table not exists!';
				if (!isset($post['id'])) $message[] = 'id not exists!';
				if (isset($post['data'])){
					$emptyData = $fields = array();$emptyField = 0;
					foreach ($this->articles_field as $field) {
						foreach ($post['data'] as $key => $value) {
							if ($key != 'id'){
								foreach ($this->mandatory_articles as $mandatkey) {
									if ($key == $mandatkey){
										if (strlen(trim($value)) == 0){
											$emptyField = 1;
											$emptyData[] = $key;
											break;
										}
									}
								}
								if ($field == $key) $fields[] = $key;
								if ($emptyField == 1) break;
							}
						}
						if ($emptyField == 1) break;
					}
					if ($emptyField == 1){
						$message[] = 'fields ('.implode(",", $emptyData).') cannot empty!';
					}else{
						if (count($fields) != count($this->articles_field)){
							 $diffKey = array_diff_key( $this->articles_field,$fields);
							 $newkeys = array();
							 foreach ($diffKey as $keydif => $valuedif) {
									$newkeys[] = $valuedif;
							 }
							 $message[] = 'fielddiff='.implode(',',$newkeys);
						}
					}
				}else{
					$message[] = 'data not exists!';
				}
		 }
		 return $message;
	}

	function moveData(){
		$this->load->library('elasticsearch_new2');
		$search = array(
			array('publish_date' => 'week')
		);
		$val = array($this->input->get('start_week'));
		$end = date('Y-m-d', strtotime($this->input->get('start_week'). ' +1 weeks'));

		$data = $this->elasticsearch_new->search('articles', $search, $val, array(), 0, 10000);
		$dataitem = $data['data'];
		// echo $end.' = '.json_encode($data, JSON_PRETTY_PRINT);
		for ($i=0; $i < count($dataitem); $i++) {
				unset($dataitem[$i]['_index']);
				unset($dataitem[$i]['_type']);
				unset($dataitem[$i]['_id']);
				unset($dataitem[$i]['_score']);
		}
		$this->elasticsearch_new2->insert('articles', $dataitem);
	}

	public function aggregate(){
		$req = $this->input->get();
		$reqkey = $newReq = $findReq = $sort = array();
		// echo json_encode($req);die();
		if (isset($req['terms']) && isset($req['termdate']) && isset($req['termrange']) && isset($req['interval']) && isset($req['table'])){
			if ((isset($req['field']) && strlen($req['field']) != 0) && (isset($req['keyword']) && strlen($req['keyword']) != 0)){
				$reqkey = explode(",",$req['field']);
				$multiple = 0;
				if ((strpos($req['keyword'], '-') !== false) || (strpos($req['field'], 'id') !== false)) {
					if (strpos($req['field'], 'date') !== false){
						$newReq[] = array(
							$reqkey[0] => 'date'
						);
					}else{
						$newReq[] = array(
							$req['field'] => 'strict'
						);
					}
				}else{
					if (strpos($req['field'], 'date') !== false){
						$newReq[] = array(
							$reqkey[0] => 'date'
						);
					}else{
						if (strpos($req['keyword'], '|') !== false){
							$newReq[] = array(
								$req['field'] => 'or'
							);

						}else{
							$multiple = 1;
							$reqfield = explode(",",$req['field']);
							$reqkeyw = explode(",",$req['keyword']);
							foreach ($reqfield as $keyf) {
								$newReq[] = array(
									$keyf => 'strict'
								);
							}
							foreach ($reqkeyw as $keyw) {
								$findReq[] = str_replace('%20',' ',$keyw);
							}
							// $newReq[] = array(
							// 	json_encode($reqkey) => 'combine_wildcard'
							// );
						}

					}
				}
				if ($multiple == 0){
					if (strpos($req['keyword'], '|') !== false){
						$findReq[] = str_replace('|',' or ',$req['keyword']);
					}else{
						$findReq[] = str_replace('%20',' ',$req['keyword']);
					}
				}
			}
			$newReq[] = array(
				$this->input->get('termdate') => 'daterange'
			);
			$findReq[] = $req['termrange'];

			$dataterms = explode(",",$this->input->get('terms'));
			$limit = ($this->input->get('limit')) ? $this->input->get('limit') : 1000;
			$data = $this->elasticsearch_new->populate($this->input->get('table'), $newReq, $findReq, $this->input->get('termdate'), $this->input->get('interval'), $dataterms, '', $limit);
			echo json_encode($data);
		}else {
			echo 'table, terms, termdate, termrange, interval mandatory';
		}

	}

	public function emptytable(){
		$result = array(
			'status' => 0,
			'message' => ''
		);
		if ($this->input->get('pass') && $this->input->get('table')){
			if ($this->authkey == $this->input->get('pass')){
						$this->elasticsearch_new->truncate($this->input->get('table'));
						$result['status'] = 1;
						$result['message'] = 'Success Empty Table '.$this->input->get('table');
			}else{
				$result['message'] = 'login pass failed!';
			}
		}
		echo json_encode($result);
	}
}
