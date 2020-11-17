<?php
set_time_limit(0);
ini_set("memory_limit","2048M");
/**
 * Elasticsearch Library Version 1.4
 *
 * @package OpenLibs
 *
 * updated library usage :
 * flow usage : create -> insert -> update -> delete -> search
 * each insert / update will be validated through check by mapping
 * each index for data will be used by one
 */
class ElasticSearch_new
{
    public $index;

    /**
     * constructor setting the config variables for server ip and index.
     */
    // protected $public $allow_type = array('text','date','long','integer','float');
    public function __construct()
    {
        $ci = &get_instance();
        $ci -> config -> load("elasticsearch");
        $this -> server = $ci -> config -> item('es_server');
        $this -> index = 'tribunnews-';//$ci -> config -> item('index');
    }
    /**
     * Handling the call for every function with curl
     *
     * @param type $path
     * @param type $method
     * @param type $data
     *
     * @return type
     * @throws Exception
     */

     public function call($indexData, $path, $method = 'GET', $data = null, $id = '', $year = '', $is_secure = 0)
     {
         $serverPoint = ($indexData == 'hotel') ? $this -> server_hotel : $this -> server;
         $indexServer = ($indexData == 'hotel') ? 'livetravel-' : $this -> index;
         if ($year != ''){
           $url = $serverPoint . '/' .$year . '.' . $indexServer.$indexData;
         }else{
           $url = $serverPoint . '/' . $indexServer.$indexData;
         }
         if ($path != '') $url .= '/'.$path;
         if ($id != '') $url .= '/'.$id;
         if ($method == 'PUT') $data = json_encode($data);
         if (is_array($data)) $data = json_encode($data);
         if ($indexData == 'hotel'){
           if ($is_secure == 0){
             $opts = array('http' =>
                 array(
                     'method'  => $method,
                     'header'  => 'Content-Type: application/json',
                     'content' => $data
                 )
             );
           }else{

             $auth = base64_encode($this->username.":".$this->password);
             $opts = array('http' =>
                 array(
                     'method'  => $method,
                     'header'  => array(
                       'Content-Type: application/json',
                       'header' => 'Authorization: Basic '.$auth
                     ),
                     'content' => $data
                 )
             );
           }
         }else{
           $opts = array('http' =>
               array(
                   'method'  => $method,
                   'header'  => 'Content-Type: application/json',
                   'content' => $data
               )
           );
         }

         $context = stream_context_create($opts);

         $response = @file_get_contents($url,false,$context);
         if ($path == '_doc'){
           // log_message('ERROR','check elastic tes 1 >>>'.json_encode($opts));
           // log_message('ERROR','check elastic tes 2 >>>'.$data);
           // log_message('ERROR','check elastic tes 3 >>>'.$url);
           // log_message('ERROR','check elastic tes 4 >>>'.$response);
         }
         if ($response){
           return json_decode($response, true);
         }else{
           $str1=$str2=$status=null;
           sscanf($http_response_header[0] ,'%s %d %s', $str1,$status, $str2);
           $error = $http_response_header[0];
           $response = array();
           $response['error'] = array();
           $response['error']['reason'] = $response['error']['type'] = $error;
           return $response;
         }
     }

     public function create($tables = array(), $year = '', $is_secure = 0){
        //validate table
        $property = '';
        $status = array('status' => array(),'message' => array());
        if (count($tables) > 0){
           $param = array();
           $success_table = array();
           foreach ($tables as $name => $field_filter) {
              $param_property = array();
              foreach ($field_filter as $field => $type) {
                 if ($type == "date"){
                    $param_property[$field] = array("type"=>"date", "format" => "yyyy-MM-dd HH:mm:ss");
                 }else if ($type == "text"){
                    $param_property[$field] = array("type"=>"text", "analyzer" => "custom_analyzer", "fielddata" =>  true);
                 }else if ($type == "integer"){
                   $param_property[$field] = array("type"=>"integer");
                 }else if ($type == "geo"){
                   $param_property[$field] = array("type"=>"geo_point");
                 }else if ($type == "keyword"){
                   $param_property[$field] = array("type"=>"keyword");
                 }else{
                   $param_property[$field] = array("type"=>"double");
                 }
              }

              $property = json_encode($param_property, JSON_PRETTY_PRINT);
              //build mapping
              $req = '{
                 "settings": {
                    "analysis": {
                       "analyzer": {
                          "custom_analyzer": {
                             "type": "custom",
                             "tokenizer": "standard",
                             "filter": [
                                "lowercase"
                             ]
                          }
                       }
                    }
                 },
                 "mappings": {
                   "properties": '.$property.'
                 }
              }';
              $response = $this->call($name,'','PUT',json_decode($req,true), '', $year, $is_secure);
              if (isset($response['acknowledged']) && $response['acknowledged'] == true){
                 $success_table[$name] = 1;
              }else{
                if (isset($response['error']['type']) && $response['error']['type'] == 'resource_already_exists_exception'){
                   //get mapping and put the new mapping
                   $err_message = array();

                   foreach ($tables as $t_name => $f_key) {
                     $mapping = $this->check_mapping($t_name, $year, $is_secure);
                     if ($year != ''){
                       $table_map = $mapping[$year.'.'.$this -> index.'-'.$t_name]['mappings']['properties'];
                     }else{
                       $table_map = $mapping[$this -> index.'-'.$t_name]['mappings']['properties'];
                     }
                     if (count($table_map) > 0){
                        $err_message[] = 'table '.$table_name.' already exists!';
                     }
                   }

                   if (count($err_message) == 0){
                      $req = '{
                        "properties": '.$property.'
                      }';
                      $response = $this->call($name,'','PUT',json_decode($req,true), '_mapping', '', $year, $is_secure);
                      if ($response['acknowledged'] == true){
                      }
                   }else{
                     // $status['message'] = implode(", ",$err_message);
                     $status['message'][] = $response['error']['root_cause']['reason'];
                   }
                }else{
                  $success_table[$name] = 0;
                  if (isset($response['error']['root_cause']['reason'])){
                     $status['message'][] = $response['error']['root_cause']['reason'];
                  }
                }

              }

           }
           $status['status'] = $success_table;
        }

        return $status;
     }

     public function check_mapping($table, $year = '', $is_secure = 0){
        $result = array('status' => 0, 'data' => array(), 'message' => '');
        $getmapping = $this->call($table, '_mapping', 'GET', array(), '', $year, $is_secure);
        // log_message('ERROR','GET MAPPING DATA >>>>>>>>>>> '.json_encode($getmapping));
        return $getmapping;
     }

     public function insert($table, $fields, $year = '', $is_secure = 0){
       //reparse data free from double quote for full texts
       $result = array('status' => 0, 'success_count' => 0, 'id' => array(), 'message' => array());
       for ($i=0; $i < count($fields); $i++) {
          $hasId = 0;

          foreach ($fields[$i] as $key => $value) {
            if (!is_array($value)){
              if (strlen($value) > 50){
                 $fields[$i][$key] = $this->reparse_in($value);
              }
            }
            if ($key == 'id') $hasId = 1;
          }
          $CheckDuplicate = 0;
          if ($hasId == 0){
            $fields[$i]['id'] = (String) $this->new_id($table, $year, $is_secure);
          }else{
            $check = $this->search($table, array(0=>array('id' => 'strict')), array($fields[$i]['id']), array(), 0, 1, 1, $year, $is_secure);
            if (isset($check['data']) && count($check['data']) > 0) $CheckDuplicate = 1;
          }
          if ($CheckDuplicate == 0){
            if ($table == 'articles'){
              if ($fields[$i]['frontpage_section'] == 1){
                $fields[$i]['frontpage_section'] = true;
              }
              if ($fields[$i]['frontpage_section'] == 0){
                $fields[$i]['frontpage_section'] = false;
              }
              if ($fields[$i]['frontpage_category'] == 1){
                $fields[$i]['frontpage_category'] = true;
              }
              if ($fields[$i]['frontpage_category'] == 0){
                $fields[$i]['frontpage_category'] = false;
              }
            }

            $response = $this->call($table, '_doc','PUT', $fields[$i], $fields[$i]['id'], $year, $is_secure);
            if (isset($response['_shards']['successful']) && $response['_shards']['successful'] > 0){
               $result['success_count'] = $result['success_count'] + 1;
               $result['id'][] = $fields[$i]['id'];
            }else{
              if (isset($response['error']['reason'])) $result['message'][] = $response['error']['reason']; else $result['message'][] = 'Failed insert on field = '.$i;
            }
          }else{
            $result['message'][] = 'Failed insert on field = '.$i.' id = '.$fields[$i]['id'].' already exists';
          }
          sleep(1);
          // exit();
       }
       if (count($fields) == $result['success_count']) $result['status'] = 1;
       // log_message('ERROR','check current success >>> '.json_encode($result));
       return $result;
     }

     public function update($table = '', $id = '', $update = array(), $year = '', $is_secure = 0){
        $result = array('status' => 0, 'message' => '');
        if ($id != ''){
          $req = array(
            'doc' => $update
          );
          $response = $this->call($table, '_update', 'POST', json_encode($req), $id, $year, $is_secure);
          if (isset($response['result']) && $response['result'] == 'updated'){
             $result['status'] = 1;
          }else{
            if (isset($response['error']['reason'])){
                $result['message'] = $response['error']['reason'];
            }else if (isset($response['result']) && $response['result'] == 'noop'){
              $result['status'] = 1;
              $result['message'] = 'no change from last save on table '.$table;
            }
          }
        }
        return $result;
     }

     public function delete($table = '', $id, $year = '', $is_secure = 0){
       $result = array('status' => 0, 'message' => '');
       $response = $this->call($table, '_doc', 'DELETE', array(), $id, $year, $is_secure);
       if (isset($response['_shards']['successful']) && $response['_shards']['successful'] > 0){
          $result['status'] = 1;
       }else{
          if (isset($response['error']['reason'])) $result['message'] = $response['error']['reason'];
       }
       return $result;
     }

     public function search($table = '', $fields = array(), $keyword = array(), $sort = array(), $start = 0, $limit = 1, $fast = 0, $year = '', $exclude = '', $is_secure = 0){
       $result = array('status' => 0, 'data' => array(), 'message' => '', 'total_rows' => 0);
       $allow_type = array('date', 'strict', 'wildcard', 'or', 'combine_wildcard', 'range', 'geo', 'week', 'ignore');
       $field_search = '[{"match_all" : {}}]';
       if (count($fields) == count($keyword)){
         $ignore_arr = $search_arr = $selector = array();

         $combinestatus = 0;

            $filterloc = '';
            for ($i=0; $i < count($fields); $i++) {
              foreach ($fields[$i] as $keyField => $typeField) {
                if (in_array($typeField, $allow_type)){
                  // foreach ($map_key as $key_name => $keyAttr) {
                      // if ($keyField == $key_name && $typeField != 'combine_wildcard'){
                      if ($typeField != 'combine_wildcard'){
                         if ($typeField == 'date'){
                           $search_arr[] = array(
                             "range" => array(
                               $keyField => array(
                                 "gte" => $keyword[$i],
                                 "lte" => date('Y-m-d', strtotime($keyword[$i]. ' +1 days')),
                                 "format" => "yyyy-MM-dd"
                               )
                             )
                           );
                         }else if ($typeField == 'or'){
                            $explSel = explode("or", $keyword[$i]);
                            $selector[0] = array();
                            foreach ($explSel as $selVal) {
                              $selector[0][] = array(
                                "match_phrase" => array(
                                  $keyField => trim($selVal)
                                )
                              );
                            }
                         }else{
                           if ($typeField == 'wildcard'){
                             $search_arr[] = array(
                               "query_string" => array(
                                 "analyze_wildcard" => true,
                                 "query" => '*'.$keyword[$i].'*', //$keyword[$i]
                                 "default_field" => $keyField
                               )
                             );
                           }else if ($typeField == 'range'){
                            $numrange = explode(",",$keyword[$i]);
                            $search_arr[] = array(
                              "range" => array(
                                $keyField => array(
                                  "gte" => $numrange[0],
                                  "lte" => $numrange[1],
                                  "format" => "yyyy-MM-dd"
                                )
                              )
                            );
                          }else if ($typeField == 'geo'){
                             $dataloc = explode('|',$keyword[$i]);
                             $measure = $dataloc[0].'m';
                             if (intval($dataloc[0]) > 1000){
                                $measure = ($dataloc[0] / 1000).'km';
                             }
                             $filterloc = ',"filter" : {
                                                "geo_distance" : {
                                                    "distance" : "'.$measure.'",
                                                    "'.$keyField.'" : {
                                                        "lat" : '.$dataloc[1].',
                                                        "lon" : '.$dataloc[2].'
                                                    }
                                                }
                                            }';
                          }else if ($typeField == 'week'){
                            $search_arr[] = array(
                              "range" => array(
                                $keyField => array(
                                  "gte" => $keyword[$i],
                                  "lte" => date('Y-m-d', strtotime($keyword[$i]. ' +1 weeks')),
                                  "format" => "yyyy-MM-dd"
                                )
                              )
                            );
                          }else if ($typeField == 'ignore'){
                            $ignore_arr[] = array(
                              "match_phrase" => array(
                                $keyField => trim($keyword[$i])
                              )
                            );
                          }else{
                            $search_arr[] = array(
                              "match_phrase" => array(
                                $keyField => trim($keyword[$i])
                              )
                            );
                          }
                         }
                      }
                  // }
                  if ($typeField == 'combine_wildcard'){
                    $combinestatus = 1;
                    $search_arr[] = array(
                        "multi_match" => array(
                          "query" => $keyword[$i],
                          "type" => 'phrase',
                          "fields" => json_decode($keyField)
                        )
                    );
                  }
                }
              }
            }
            $textPagin = $sortString = '';
            if ($limit > 0) $textPagin = '"from" : '.$start.', "size" : '.$limit.',';
            $sortArray = array();
            if (count($sort) > 0){
              if (isset($sort['field']) && isset($sort['type'])){
                 if ($sort['field'] == 'location'){
                  // $dataloc = explode('|',$keyword[$i]);
                    $sortString = '[
                                     "_score",
                                     {
                                       "_geo_distance": {
                                         "location": {
                                             "lat": '.$dataloc[1].',
                                             "lon": '.$dataloc[2].'
                                         },
                                         "order":         "'.$sort['type'].'",
                                         "unit":          "m",
                                         "distance_type": "arc"
                                       }
                                     }
                                  ]';
                 }else{
                    if ((strtolower($sort['type']) == 'asc') || (strtolower($sort['type']) == 'desc')) $sortArray = array(array($sort['field'] => array("order" => strtolower($sort['type']))));
                 }

              }

            }
            $excl = '';
            if ($exclude != ''){
               $dataexc = explode(",",$exclude);
               $strexcl = '';
               foreach ($dataexc as $valueex) {
                  $strexcl .= ($strexcl == '') ? '"'.$valueex.'"' : ',"'.$valueex.'"';
               }
               $excl = '"_source":{
                 "excludes":['.$strexcl.']
               },';
            }
            $queryFinal = '';

            $queryIgnore = '';
            if (count($ignore_arr) > 0){
               $queryIgnore = ',"must_not" : '.json_encode($ignore_arr).',';
            }
            if (isset($selector[0])){
              if (count($selector[0]) > 0){
                $search_arr[] = array('bool' => array('should' => $selector[0]));
              }
            }

            $queryFinal = '"query": {
             "bool": {
               "must": '.json_encode($search_arr, JSON_PRETTY_PRINT).'
               '.$queryIgnore.'
               '.$filterloc.'
             }
           }';

             //  $queryFinal = '"query": {
             //   "bool": {
             //     "must": '.json_encode($search_arr, JSON_PRETTY_PRINT).',
             //     '.$queryIgnore.'
             //     "should": '.json_encode($selector, JSON_PRETTY_PRINT).'
             //     '.$filterloc.'
             //   }
             // }';

            if ($sortString == ''){
              $req = '{
                        '.$excl.'
                        '.$textPagin.'
                        '.$queryFinal.',
                      "sort": '.json_encode($sortArray, JSON_PRETTY_PRINT).'
                    }';
            }else{
              $req = '{
                        '.$excl.'
                        '.$textPagin.'
                        '.$queryFinal.',
                      "sort": '.$sortString.'
                    }';
            }

            $response = $this->call($table,'_search', 'GET', $req, '', $year, $is_secure);

            if (isset($response['hits']['hits']) && count($response['hits']['hits']) > 0){
              $result['status'] = 1;
              $result['data'] = $response['hits']['hits'];
              for ($i=0; $i < count($result['data']); $i++) {
                 foreach ($result['data'][$i]['_source'] as $key => $value) {
                   if (!(is_array($value))){
                     if (strlen($value) > 50){
                        $result['data'][$i][$key] = $this->reparse_out($value);
                     }else{
                       $result['data'][$i][$key] = $value;
                     }
                   }else{
                     $result['data'][$i][$key] = $value;
                   }
                 }
                 if (isset($result['data'][$i]['sort'][1])){
                   $result['data'][$i]['distance'] = 0;
                    $result['data'][$i]['distance'] = $result['data'][$i]['sort'][1];
                 }
                 unset($result['data'][$i]['sort']);
                 unset($result['data'][$i]['_source']);
              }
              $result['total_rows'] = $response['hits']['total']['value'];
            }
         // }else{
         //   $result['message'] = 'table for search not exists';
         // }
       }else{
         $result['message'] = 'number of fields and keyword should be same';
       }
       return $result;
     }

     public function new_id($table = '', $year = '', $is_secure = 0){
       $id = 1;
       $req = '{
          "from": 0, "size": 1,
          "_source": "id",
            "query": {
             "match_all": {}
            },
          "sort": [
            {
              "id": {
                "order": "desc"
              }
            }
          ]
        }';

       $resp = $this->call($table,'_search', 'GET', $req, '', $year, $is_secure);

       if (isset($resp['hits'][0]['_source']['id'])){
          $id = intval($resp['hits'][0]['_source']['id'])+1;
       }else{
         if (isset($resp['hits']['hits'][0]['_source']['id'])){
           $id = intval($resp['hits']['hits'][0]['_source']['id'])+1;
         }
       }
       log_message('ERROR','NEW ID >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'.json_encode($resp));
       return $id;
     }

     function reparse_in($text = ''){
       if (strpos($text, '^') === false){
          $text = str_replace('"', '^',$text);
       }else{
         if (strpos($text, '`') === false){
           $text = str_replace('"', '^',$text);
         }
       }
       return $text;
     }

     function reparse_out($text = ''){
       if (strpos($text, '^') !== false){
          $text = str_replace('^', '"',$text);
       }else{
         if (strpos($text, '`') !== false){
           $text = str_replace('`', '"',$text);
         }
       }
       return $text;
     }

     public function truncate($table = '', $fields = array(), $year = '', $is_secure = 0){
       if ($table != ''){
         if (count($fields) > 0){
           foreach ($fields as $key => $value) {
             $req = '{
                        "query": {
                          "match_phrase": {
                            "'.$key.'": "'.$value.'"
                          }
                        }
                      }';
           }

         }else{
           $req = '{
                      "query": {
                        "match_all": {}
                      }
                    }';
         }

         $resp = $this->call($table,'_delete_by_query?conflicts=proceed', 'POST', $req, '', $year, $is_secure);
       }
     }

     public function populate($table = '', $fields = array(), $keyword = array(),$termdate = '', $interval = 'week', $terms = array(), $year = '', $limit = 1000){
       $result = array('status' => 0, 'data' => array(), 'message' => '', 'time' => 0);
       $allow_type = array('daterange', 'strict', 'wildcard', 'or', 'combine_wildcard');
       $field_search = '[{"match_all" : {}}]';
       $stringQuery = '';
       if (count($fields) == count($keyword)){
         $search_arr = array();$selector = array();
         for ($i=0; $i < count($fields); $i++) {
           foreach ($fields[$i] as $keyField => $typeField) {
             if (in_array($typeField, $allow_type)){
               // foreach ($map_key as $key_name => $keyAttr) {
                   // if ($keyField == $key_name && $typeField != 'combine_wildcard'){
                   if ($typeField != 'combine_wildcard'){
                      if ($typeField == 'daterange'){
                        $explSel = explode(".", $keyword[$i]);
                        $search_arr[] = array(
                          "range" => array(
                            $keyField => array(
                              "gte" => $explSel[0],
                              "lte" => $explSel[1],
                              "format" => "yyyy-MM-dd"
                            )
                          )
                        );
                      }else if ($typeField == 'or'){
                         $explSel = explode("or", $keyword[$i]);
                         $selector[0] = array();
                         foreach ($explSel as $selVal) {
                           $selector[0][] = array(
                             "match_phrase" => array(
                               $keyField => trim($selVal)
                             )
                           );
                         }
                      }else{
                        if ($typeField == 'wildcard'){
                          $search_arr[] = array(
                            "query_string" => array(
                              "analyze_wildcard" => true,
                              "query" => '*'.$keyword[$i].'*', //$keyword[$i]
                              "default_field" => $keyField
                            )
                          );
                        }else{

                            $search_arr[] = array(
                              "match_phrase" => array(
                                $keyField => trim($keyword[$i])
                              )
                            );


                        }
                      }
                   }
               // }
               if ($typeField == 'combine_wildcard'){
                 $search_arr[] = array(
                   "query_string" => array(
                     "analyze_wildcard" => true,
                     "query" => '*'.$keyword[$i].'*', //$keyword[$i]
                     "fields" => json_decode($keyField)
                   )
                 );
               }
             }
           }
         }
         if (count($search_arr) > 0){
            $stringQuery = '"query": {
                              "bool": {
                                "must": '.json_encode($search_arr, JSON_PRETTY_PRINT).'
                              }
                          },';
         }
       }
       $stringGroup = '';
       foreach ($terms as $keyterm) {
          if ($stringGroup != ''){
             $stringGroup .= ',';
          }
          $stringGroup .= '"group_by_'.$keyterm.'": {
                              "terms": {
                                "field": "'.$keyterm.'",
                                "size": "'.$limit.'"
                              }
                          }';
       }
       $queryAggs = '{
                    "size": 0,
                    '.$stringQuery.'
                    "aggs": {
                      "group_by_month": {
                        "date_histogram": {
                          "field": "'.$termdate.'",
                          "interval": "'.$interval.'"
                        },
                        "aggs": {
                          '.$stringGroup.'
                        }
                      }
                    }
                  }';
          $response = $this->call($table,'_search', 'GET', $queryAggs, '', $year);
          if (isset($response['aggregations']['group_by_month']['buckets'])){
             $result['status'] = 1;
             $dataRange = $response['aggregations']['group_by_month']['buckets'];
             for ($j=0; $j < count($dataRange); $j++) {
                 $daterange[$j]['period'] = date('Y-m-d', strtotime($dataRange[$j]['key_as_string']));
                 unset($dataRange[$j]['key_as_string']);
                 unset($dataRange[$j]['doc_count_error_upper_bound']);
             }
             $result['data'] = $dataRange;
             $result['time'] = $response['took'];
          }
          return $result;
     }



}
