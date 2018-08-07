<?php

namespace Honvid;

/*
|--------------------------------------------------------------------------
| CLASS NAME: Thrift
|--------------------------------------------------------------------------
| @author    honvid
| @datetime  2018-06-04 17:27
| @package   Honvid
| @description: this package is combine the thrift, hbase to the laravel.
|
*/

use Log;
use Illuminate\Config\Repository;
use Thrift\Transport\TSocket;
use Honvid\Hbase\Mutation;
use Thrift\Protocol\TBinaryProtocol;
use Honvid\Hbase\HbaseClient;
use Honvid\Hbase\BatchMutation;
use Thrift\Transport\TFramedTransport;

class Thrift
{
    public $client;
    public $protocol;
    public $transport;
    public $callCount = 0;
    public $startRow = 0;
    public $reqTimeAlarm;
    public $scanRetryLimitNum = 5;
    public $scanLimitNum = 50;
    protected $config;

    public function __construct(Repository $config)
    {
        $this->config = $config;
        $this->scanLimitNum = $this->config->get('thrift.scan_limit_number', 50);
        $this->scanRetryLimitNum = $this->config->get('thrift.scan_retry_limit_number', 5);
        try {
            $socket = new TSocket($this->config->get('thrift.host', '127.0.0.1'),
                $this->config->get('thrift.port', '9090'),
                $this->config->get('thrift.persist', false));
            $socket->setSendTimeout($this->config->get('thrift.send_timeout', 1500));
            $socket->setRecvTimeout($this->config->get('thrift.recv_timeout', 1500));

            $this->transport = new TFramedTransport($socket);
            $this->protocol = new TBinaryProtocol($this->transport);
            $this->client = new HbaseClient($this->protocol);
            $this->callCount = 0;
            $this->transport->open();
        } catch (\Exception $exception) {
            $this->close();
            throw new \Exception('thrift connect error!', 500);
        }
    }

    /**
     * 获取单行单列值
     *
     * @param $tableName
     * @param $columnName
     * @param $rowName
     * @return bool
     */
    public function getValue($tableName, $columnName, $rowName)
    {
        $ret = $this->client->get($tableName, $rowName, $columnName, []);
        if ($ret) {
            return current($ret)->value;
        }

        return false;
    }

    /**
     * 获取多行单列值
     *
     * @param $tableName
     * @param $columnName
     * @param $rowNames
     * @return array
     */
    public function getValues($tableName, $columnName, $rowNames)
    {
        $this->reqTimeAlarm = count($rowNames) * 1;

        $rows = $this->client->getRowsWithColumns($tableName, $rowNames, [$columnName], []);

        $ret = [];
        foreach ($rows as $row) {
            if (isset($row->columns[$columnName])) {
                $ret[$row->row] = $row->columns[$columnName]->value;
            }
        }

        return $ret;
    }

    /**
     * 获取单行多列值
     *
     * @param $tableName
     * @param $columnNames
     * @param $rowName
     * @return array|bool
     */
    public function getValueWithColumns($tableName, $columnNames, $rowName)
    {
        $ret = [];
        $res = $this->client->getRowWithColumns($tableName, $rowName, $columnNames, []);
        if ($res) {
            $row = current($res);
            if ($row->row !== $rowName) {
                return false;
            }
            foreach ($columnNames as $columnName) {
                if (isset($row->columns[$columnName])) {
                    $ret[$columnName] = $row->columns[$columnName]->value;
                }
            }
        }

        return $ret;
    }

    /**
     * 得到多行多列值
     *
     * @param $tableName
     * @param $columnNames
     * @param $rowNames
     * @return array
     */
    public function getValuesWithColumns($tableName, $columnNames, $rowNames)
    {
        $this->reqTimeAlarm = count($rowNames) * 1;

        $rows = $this->client->getRowsWithColumns($tableName, $rowNames, $columnNames, []);

        $ret = [];
        foreach ($rows as $row) {
            foreach ($columnNames as $columnName) {
                if (isset($row->columns[$columnName])) {
                    $ret[$row->row][$columnName] = $row->columns[$columnName]->value;
                }
            }
        }

        return $ret;
    }

    /**
     *
     * @param          $tableName
     * @param array    $columns
     * @param callable $callback
     */
    public function scan($tableName, $columns = [], callable $callback)
    {
        $i = 0;
        while ($i < $this->scanRetryLimitNum) {
            if ( ! $this->_scan($tableName, $columns, $callback)) {
                $i++;
            }
        }

    }

    /**
     * 设置单行单列值
     *
     * @param $tableName
     * @param $columnName
     * @param $rowName
     * @param $value
     * @return bool
     */
    public function setValue($tableName, $columnName, $rowName, $value)
    {
        $row = [new Mutation(['column' => $columnName, 'value' => $value])];

        $this->client->mutateRow($tableName, $rowName, $row, []);

        return true;
    }

    /**
     * 设置多行单列值
     *
     * @param $tableName
     * @param $columnName
     * @param $rowNameValueArr
     * @return bool
     */
    public function setValues($tableName, $columnName, $rowNameValueArr)
    {
        $this->reqTimeAlarm = count($rowNameValueArr) * 2;

        if ( ! is_array($rowNameValueArr)) {
            return false;
        }

        $rowBatches = [];
        foreach ($rowNameValueArr as $rowName => $value) {
            $batchMutation = new BatchMutation();
            $batchMutation->row = $rowName;
            $batchMutation->mutations = [new Mutation(['column' => $columnName, 'value' => $value])];
            $rowBatches[] = $batchMutation;
        }

        $this->client->mutateRows($tableName, $rowBatches, []);

        return true;
    }

    /**
     * 设置单行多列值
     *
     * @param $tableName
     * @param $columnNameValueArr
     * @param $rowName
     * @return bool
     */
    public function setValueWithColumns($tableName, $columnNameValueArr, $rowName)
    {
        if ( ! is_array($columnNameValueArr) || ! $columnNameValueArr) {
            return false;
        }

        $row = [];
        foreach ($columnNameValueArr as $columnName => $value) {
            $row[] = new Mutation(['column' => $columnName, 'value' => $value]);
        }

        $this->client->mutateRow($tableName, $rowName, $row, []);

        return true;
    }

    /**
     * 删除指定行列
     *
     * @param $tableName
     * @param $rowName
     * @param $column
     */
    public function deleteByRowColumn($tableName, $rowName, $column)
    {
        $this->client->deleteAll($tableName, $rowName, $column, []);
    }

    /**
     * 删除指定行
     *
     * @param $tableName
     * @param $rowName
     */
    public function deleteByRow($tableName, $rowName)
    {
        $this->client->deleteAllRow($tableName, $rowName, []);
    }

    /**
     * 关闭连接
     */
    public function close()
    {
        if ($this->transport) {
            $this->transport->close();
        }
    }

    /**
     * @param          $tableName
     * @param array    $columns
     * @param callable $callback
     * @return bool
     */
    private function _scan($tableName, $columns = [], callable $callback)
    {
        $scanId = $this->client->scannerOpen($tableName, $this->startRow, $columns, []);
        try {
            while ($list = $this->client->scannerGetList($scanId, $this->scanLimitNum)) {
                foreach ($list as $result) {
                    $this->startRow = $result->row;
                    //yield $result->columns;
                    foreach ($columns as $column) {
                        $value = $result->columns[$column]->value ?? [];
                        $callback($value);
                    }
                }
            };

            $this->client->scannerClose($scanId);
        } catch (\Exception $exception) {
            $this->client->scannerClose($scanId);
            Log::warning("THRIFT_SCAN_ERROR", $exception->getMessage());

            return false;
        }

        return true;
    }
}