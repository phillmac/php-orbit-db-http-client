<?php
namespace OrbitdbClient;

use Exception;
use function DeepCopy\deep_copy;
use SseClient\EventStream as SseEventStream;

class DB  {
    private $cache = [];
    private $params;
    private $options;
    private $db_options;
    private $capabilities;
    private $id;
    private $id_safe;
    private $dbname;
    private $type;
    private $func_call;
    private $func_call_raw;
    private $base_url;
    private $use_cache;
    private $enforce_caps;
    private $enforce_indexby;

    public function __construct(array $params, array $options) {
        $this->params           = $params;
        $this->options          = $options;
        $this->db_options       = $params['options'];
        $this->capabilities     = $params['capabilities'];
        $this->id               = $params['id'];
        $this->id_safe          = urlencode($this->id);
        $this->dbname           = $params['dbname'];
        $this->type             = $params['type'];
        $this->func_call         = $options['call'];
        $this->func_call_raw     = $options['call_raw'];
        $this->base_url         = $options['base_url'];
        $this->use_cache        = $options['use_db_cache'];
        $this->enforce_caps     = $options['enforce_caps'];
        $this->enforce_indexby  = $options['enforce_indexby'];


    }

    public function clearCache()
    {
        $this->cache = [];
    }

    public function cacheGet($item)
    {
        $item = strval($item);
        if (isset($this->cache[$item])) return deep_copy($this->cache[$item]);
    }

    public function cacheDelete($item)
    {
        $item = strval($item);
        unset($this->cache[$item]);
    }

    public function __get($name=null)
    {
        switch ($name) {
            case 'index_by':
                return $this->db_options['indexBy'];
            case 'db_info':
                return deep_copy($this->db_info);
            case 'cache':
                return deep_copy($this->cache);
            case 'queryable':
                return in_array('query', $this->capabilities);
            case 'putable':
                return in_array('put', $this->capabilities);
            case 'removeable':
                return in_array('remove', $this->capabilities);
            case 'iterable':
                return in_array('iterator', $this->capabilities);
            case 'addable':
                return in_array('add', $this->capabilities);
            case 'incrementable':
                return in_array('value', $this->capabilities);
            case 'indexed':
                return in_array('indexBy', $this->db_options);
            default:
                return $this->$name ?? null;
        }
    }

    private function api_call (string $method, string $endpoint, array $json=[], array $options=[]) {
        $func_call = $this->func_call;
        return $func_call($method, $endpoint, $json, $options);
    }


    private function api_call_raw (string $method, string $endpoint, array $json=[], array $options=[]) {
        $func_call_raw = $this->func_call_raw;
        return $func_call_raw($method, $endpoint, $json, $options);
    }

    private function unpack_result($result) {
        return deep_copy($result);
    }

    public function info() {
        $endpoint = join('/',['db', $this->id_safe]);
        return $this->api_call('GET', $endpoint);
    }

    public function get(string $item, bool $cache=null, bool $unpack=FALSE) {
        $result = null;

        if (is_null($cache)) {
            $cache = $this->use_cache;
        }

        if ($cache && in_array($item, $cache)) {
            $result = $this->cache[$item];
        } else {
            $endpoint = join('/', ['db', $this->id_safe, $item]);
            $result = $this->api_call('GET', $endpoint);
            $this->cache[$item] = $result;
            if($unpack){
                return $this->unpack_result($result);
            }
            return deep_copy($result);
        }
    }

    public function get_raw(string $item) {
        return $this->get($item, FALSE, FALSE);
    }

    public function put(array $item, bool $cache=null) {
        if($this->enforce_caps && (! $this->putable)) {
            throw new CapabilityError("Db {$this->dbname} does not have put capability");
        }


        if ($this->enforce_indexby && $this->indexed && in_array($this->index_by, $item)) {
            throw new MissingIndexError("The provided document doesn't contain field '{$this->index_by}'");
        }

        if (is_null($cache)) {
            $cache = $this->use_cache;
        }

        if ($cache){
            $index_val = null;
            if ($this->indexed && in_array($this->index_by, $item)) {
                $index_val = $item[$this->index_by];
            } elseif (in_array('key', $item)) {
                $index_val = $item['key'];
            }

            if ($index_val) {
                $this->cache[$index_val] = $item;
            }

            $endpoint = join('/', [db, $this->id_safe, 'put']);
            $entry_hash = $this->api_call('POST', $endpoint, $item)['hash'] ?? '';
            if ($cache && $entry_hash){
                $cache[$entry_hash] = $item;
            }
            return $entry_hash;
        }
    }

    public function add(array $item, bool $cache=null) {
        if ($this->enforce_caps && (! $this->addable)) {
            throw new CapabilityError("Db {$this->dbname} does not have add capability");
        }

        if (is_null($cache)) {
            $cache = $this->use_cache;
        }

        $endpoint = join('/', ['db', $this->id_safe, 'add']);
        $entry_hash = $this->api_call('POST', $endpoint, $item)['hash'] ?? '';
        if ($cache && $entry_hash) {
            $cache[$entry_hash] = $item;
        }
        return $entry_hash;
    }

    public function iterator_raw (array $options) {
        if ($this->enforce_caps && (! $this->iterable)) {
            throw new CapabilityError("Db {$this->dbname} does not have iterator capability");
        }

        $endpoint = join('/', ['db', $this->id_safe, 'rawiterator']);
        return $this->api_call('GET', $endpoint, $options);
    }

    public function iterator (array $options) {
        if ($this->enforce_caps && (! $this->iterable)) {
            throw new CapabilityError("Db {$this->dbname} does not have iterator capability");
        }

        $endpoint = join('/', ['db', $this->id_safe, 'iterator']);
        return $this->api_call('GET', $endpoint, $options);
    }

    public function index () {
        $endpoint = join('/', ['db', $this->id_safe, 'index']);
        return $this->api_call('GET', $endpoint);
    }

    public function all () {
        $endpoint = join('/', ['db', $this->id_safe, 'all']);
        $result =  $this->api_call('GET', $endpoint);
        if(is_array($result)){
            $this->cache = $result;
        }
        return $result;
    }

    public function remove (string $item) {
        if ($this->enforce_caps && (! $this->removeable)) {
            throw new CapabilityError("Db {$this->dbname} does not have remove capability");
        }

        $endpoint = join('/', ['db', $this->id_safe, $item]);
        return $this->api_call('DELETE', $endpoint);
    }

    public function unload () {
        $endpoint = join('/', ['db', $this->id_safe]);
        return $this->api_call('DELETE', $endpoint);
    }


    public function events($eventnames, $callback) {
        if (is_array($eventnames)){
            $eventnames = join(',', $eventnames);
        }
        $endpoint = join('/', ['db', $this->id_safe, urlencode($eventnames)]);
        $eventstream = new EventStream(function ($event) use ($callback) {
            $callback($event);
        });

        $this->api_call_raw('GET' ,$endpoint, [
            'sink' => $eventstream
        ]);
    }
}


class CapabilityError extends Exception {}
class MissingIndexError extends Exception {}