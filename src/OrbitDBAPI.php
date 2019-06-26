<?php
namespace OrbitdbClient;

use OrbitdbClient\DB as DB;
use Exception;
use GuzzleHttp\Client as GuzzleClient;


class OrbitDBAPI
{
    private $base_uri;
    private $timeout;
    private $useDBCache;
    private $debug;
    private $client;

    public function __construct(string $base_uri, int $timeout=30, bool $useDBCache=false, array $guzzle_config=[], bool $debug=false)
    {
        $this->base_uri     = $base_uri;
        $this->timeout      = $timeout;
        $this->useDBCache   = $useDBCache;
        $this->debug        = $debug;
        $this->client       = new GuzzleClient(array_merge([
            'base_uri' =>  $base_uri,
            'timeout' => $timeout,
            'headers' => [
                'Cache-Control' => 'no-cache'
            ],
            'curl' => [
                CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_2TLS,
            ],
        ], $guzzle_config));
    }

    private function do_request(string $method, string $url, array $json=[], array $options=[])
    {
        return $this->client->request($method, $url, array_merge([
                'json' => $json,
                'connect_timeout' => $this->timeout,
                'debug' => $this->debug
            ], $options)
        );
    }

    private function call_raw(string $method, string $endpoint, array $json=[], array $options=[])
    {
        $url = join('/', [$this->base_uri, $endpoint]);
        return $this->do_request($method, $endpoint, $json, $options);
    }

    private function call(string $method, string $endpoint, array $json=[], array $options=[])
    {
        $response = null;
        try {
            $response = $this->call_raw($method, $endpoint, $json, $options);
        } catch (Exception $e){
            trigger_error("Exception in request: $e", E_USER_WARNING);
        } if ($response)  {
            $response_contents = $response->getBody();
            try {
                $result = json_decode($response_contents, TRUE);
                assert(! is_null($response), new Exception('Empty json response body'));
                var_dump($result)
                return $result;
            } catch (Exception $e){
                trigger_error("Exception in json decode: $e", E_USER_WARNING);
            }
        }

    }

    private function get_config()
    {
        return array(
            'base_uri'      => $this->base_uri,
            'timeout'       => $this->timeout,
            'useDBCache'    => $this->useDBCache,
            'debug'         => $this->debug,
            'client'        => $this->client,
            'call'          => function (string $method, string $endpoint, array $json=[], array $options=[])
            {
                $this->call($method, $endpoint, $json, $options);
            },
            'call_raw'      => function (string $method, string $endpoint, array $json=[], array $options=[]) {
                $this->call_raw($method, $endpoint, $json, $options);
            }
        );
    }

    public function list_dbs() {
        return $this->call('GET', 'dbs');
    }

    public function open_db(string $db_name, array $db_options)
    {
        $endpoint = join('/',['db', urlencode($db_name)]);
        return $this->call('POST', $endpoint, $db_options);
    }

    public function db(string $db_name, array $db_options)
    {
        $db_params = $this->open_db($db_name, $db_options);
        return new DB($db_params, $this->get_config());
    }
}
