<?php
set_time_limit(0);
ini_set("memory_limit","2048M");
/**
 * Elasticsearch Library
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
        $this -> index = 'tribunnews';//$ci -> config -> item('index');
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

     public function call($indexData, $path, $method = 'GET', $data = null, $id = '', $year = '')
     {
         if ($year != ''){
           $url = $this -> server . '/' .$year . '.' . $this -> index.'-'.$indexData;
         }else{
           $url = $this -> server . '/' . $this -> index.'-'.$indexData;
         }
         if ($path != '') $url .= '/'.$path;
         if ($id != '') $url .= '/'.$id;
         if ($method == 'PUT') $data = json_encode($data);
         if (is_array($data)) $data = json_encode($data);
         $opts = array('http' =>
             array(
                 'method'  => $method,
                 'header'  => 'Content-Type: application/json',
                 'content' => $data
             )
         );
         $context = stream_context_create($opts);

         // echo $data;
         // echo $url;
         // die();
         $response = @file_get_contents($url,false,$context);
         // echo $data;
         // echo $url;
         // die();
         log_message('ERROR','check elastic tes 2 >>>'.$data);
         log_message('ERROR','check elastic tes 3 >>>'.$url);
         log_message('ERROR','check elastic tes 4 >>>'.$response);
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

     public function create($tables = array(), $year = ''){
        //validate table
        $property = '';
        $status = array('status' => array(),'message' => array());
        if (count($tables) > 0){
           $param = array();
           $success_table = array();
           foreach ($tables as $name => $field_filter) {
              $param_property = array();
              foreach ($field_filter as $field => $type) {
                 if ($type != "date"){
                    $param_property[$field] = array("type"=>"text", "analyzer" => "custom_analyzer", "fielddata" =>  true);
                 }else{
                    $param_property[$field] = array("type"=>"date", "format" => "yyyy-MM-dd HH:mm:ss");
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
              $response = $this->call($name,'','PUT',json_decode($req,true), '', $year);
              if (isset($response['acknowledged']) && $response['acknowledged'] == true){
                 $success_table[$name] = 1;
              }else{
                if (isset($response['error']['type']) && $response['error']['type'] == 'resource_already_exists_exception'){
                   //get mapping and put the new mapping
                   $err_message = array();

                   foreach ($tables as $t_name => $f_key) {
                     $mapping = $this->check_mapping($t_name, $year);
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
                      $response = $this->call($name,'','PUT',json_decode($req,true), '_mapping', '', $year);
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

     public function check_mapping($table, $year = ''){
        $result = array('status' => 0, 'data' => array(), 'message' => '');
        $getmapping = $this->call($table, '_mapping', 'GET', array(), '', $year);
        return $getmapping;
     }

     public function insert($table, $fields, $year = ''){
       //reparse data free from double quote for full texts
       $result = array('status' => 0, 'success_count' => 0, 'id' => array(), 'message' => array());
       for ($i=0; $i < count($fields); $i++) {
          $hasId = 0;
          foreach ($fields[$i] as $key => $value) {
            if ($value > 50){
               $fields[$i][$key] = $this->reparse_in($value);
            }
            if ($key == 'id') $hasId = 1;
          }
          $CheckDuplicate = 0;
          if ($hasId == 0){
            $fields[$i]['id'] = (String) $this->new_id($table, $year);
          }
          // }else{
          //   $check = $this->search($table, array(0=>array('id' => 'strict')), array($fields[$i]['id']), array(), 0, 1, 1, $year);
          //   if (isset($check['data']) && count($check['data']) > 0) $CheckDuplicate = 1;
          // }
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

            if (isset($fields[$i]['fulltexts'])){
              $fields[$i]['fulltexts'] = trim($fields[$i]['fulltexts']);
            }

            // echo json_encode($fields);
            // die();

            $response = $this->call($table, '_doc','PUT', $fields[$i], $fields[$i]['id'], $year);
            if (isset($response['_shards']['successful']) && $response['_shards']['successful'] > 0){
               $result['success_count'] = $result['success_count'] + 1;
               $result['id'][] = $fields[$i]['id'];
            }else{
              if (isset($response['error']['reason'])) $result['message'][] = $response['error']['reason']; else $result['message'][] = 'Failed insert on field = '.$i;
            }
          }else{
            $result['message'][] = 'Failed insert on field = '.$i.' id = '.$fields[$i]['id'].' already exists';
          }
          // sleep(1);
       }
       if (count($fields) == $result['success_count']) $result['status'] = 1;
       return $result;
     }

     public function update($table = '', $id = '', $update = array(), $year = ''){
        $result = array('status' => 0, 'message' => '');
        if ($id != ''){
          $req = array(
            'doc' => $update
          );
          // echo json_encode($req).' --- '.$id;die();
          $response = $this->call($table, '_update', 'POST', json_encode($req), $id, $year);
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

     public function delete($table = '', $id, $year = ''){
       $result = array('status' => 0, 'message' => '');
       $response = $this->call($table, '_doc', 'DELETE', array(), $id, $year);
       if (isset($response['_shards']['successful']) && $response['_shards']['successful'] > 0){
          $result['status'] = 1;
       }else{
          if (isset($response['error']['reason'])) $result['message'] = $response['error']['reason'];
       }
       return $result;
     }

     public function search($table = '', $fields = array(), $keyword = array(), $sort = array(), $start = 0, $limit = 1, $fast = 0, $year = ''){
       $result = array('status' => 0, 'data' => array(), 'message' => '', 'total_rows' => 0);
       $allow_type = array('date', 'strict', 'wildcard', 'or', 'combine_wildcard', 'datetime');
       $field_search = '[{"match_all" : {}}]';
       if (count($fields) == count($keyword)){
         $search_arr = array();$selector = array();
         // $tb_map = $this->check_mapping($table, $year);
         // if ($year != ''){
         //   $key_map = $year.'.'.$this -> index.'-'.$table;
         // }else{
         //   $key_map = $this -> index.'-'.$table;
         // }
         // if (isset($tb_map[$key_map]['mappings']['properties']) && count($tb_map[$key_map]['mappings']['properties']) > 0){
            // $map_key = $tb_map[$key_map]['mappings']['properties'];
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
                         }else if ($typeField == 'datetime'){
                             $rangedatetime = explode('^',$keyword[$i]);
                             $search_arr[] = array(
                               "range" => array(
                                 $keyField => array(
                                   "gte" => $rangedatetime[0],
                                   "lte" => $rangedatetime[1]
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
            $textPagin = '';
            if ($limit > 0) $textPagin = '"from" : '.$start.', "size" : '.$limit.',';
            $sortArray = array();
            if (count($sort) > 0){
              if (isset($sort['field']) && isset($sort['type'])){
                 if ((strtolower($sort['type']) == 'asc') || (strtolower($sort['type']) == 'desc')) $sortArray = array(array($sort['field'] => array("order" => strtolower($sort['type']))));
              }
            }
            if (isset($selector[0])){
              if (count($selector[0]) > 0){
                $search_arr[] = array('bool' => array('should' => $selector[0]));
              }
            }

            $req = '{
                      '.$textPagin.'
                      "query": {
                       "bool": {
                         "must": '.json_encode($search_arr, JSON_PRETTY_PRINT).'
                       }
                      },
                    "sort": '.json_encode($sortArray, JSON_PRETTY_PRINT).'
                  }';
// echo $req;die();
            $response = $this->call($table,'_search', 'GET', $req, '', $year);

            if (isset($response['hits']['hits']) && count($response['hits']['hits']) > 0){
              $result['status'] = 1;
              $result['data'] = $response['hits']['hits'];
              for ($i=0; $i < count($result['data']); $i++) {
                 foreach ($result['data'][$i]['_source'] as $key => $value) {
                    if (count($value) > 50){
                       $result['data'][$i][$key] = $this->reparse_out($value);
                    }else{
                      $result['data'][$i][$key] = $value;
                    }
                 }
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

     public function new_id($table = '', $year = ''){
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

       $resp = $this->call($table,'_search', 'GET', $req, '', $year);

       if (isset($resp['hits'][0]['_source']['id'])){
          $id = intval($resp['hits'][0]['_source']['id'])+1;
       }else{
         if (isset($resp['hits']['hits'][0]['_source']['id'])){
           $id = intval($resp['hits']['hits'][0]['_source']['id'])+1;
         }
       }
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

     function alter($table, $field){
       $req = array(
         'properties' => $field
       );
       echo json_encode($req);
       $response = $this->call($table,'_mapping','PUT',$req, '', '', 0);
       return $response;
     }


     // public function populate($table, $field = array()){
     //   $resp = array();
     //   if (count($field) > 0){
     //     $strar = '';
     //     $indx = 0;
     //     foreach ($field as $val) {
     //          $poparr = array();
     //          $poparr[$val.'_populer'] = array('terms' => array('field' => $val));
     //          $encarr = substr(json_encode($poparr),1,(strlen(json_encode($poparr))-2));
     //          if ($indx == 0){
     //             $strar = $encarr;
     //          }else{
     //            $strar .= ','.$encarr;
     //          }
     //          $indx++;
     //     }
     //
     //     $stringReq = '{
     //        "_source": false,
     //        "size": 0,
     //          "aggs" : {'.$strar.'}
     //      }';
     //      $resp = $this->call($table,'_search', 'GET', $stringReq, '', '');
     //   }
     //   return $resp['aggregations'];
     // }
}
