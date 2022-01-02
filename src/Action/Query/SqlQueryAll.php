<?php
/*
 * Fusio
 * A web-application to create dynamically RESTful APIs
 *
 * Copyright (C) 2015-2020 Christoph Kappestein <christoph.kappestein@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

namespace Fusio\Adapter\Sql\Action\Query;

use Doctrine\DBAL\Connection;
use Fusio\Engine\ContextInterface;
use Fusio\Engine\Exception\ConfigurationException;
use Fusio\Engine\Form\BuilderInterface;
use Fusio\Engine\Form\ElementFactoryInterface;
use Fusio\Engine\ParametersInterface;
use Fusio\Engine\RequestInterface;

/**
 * SqlSelect
 *
 * @author  Christoph Kappestein <christoph.kappestein@gmail.com>
 * @license http://www.gnu.org/licenses/agpl-3.0
 * @link    http://fusio-project.org
 */
class SqlQueryAll extends SqlQueryAbstract
{
    const FILTERBY_REGEX = '/\{filterBy\}/';

    public function getName()
    {
        return 'SQL-Query-All';
    }

    public function handle(RequestInterface $request, ParametersInterface $configuration, ContextInterface $context)
    {
        $connection = $this->getConnection($configuration);

        $sql   = $configuration->get('sql');
        $limit = (int) $configuration->get('limit');
        $filteringColumns = $configuration->get('filteringColumns');

        if ((bool)$configuration->get('builtinFiltering') == true) {
            [$sql, $filterByParams] = $this->addFilter($request, $sql, $filteringColumns);
        }

        error_log(print_r($sql, TRUE), 0);
        error_log(print_r($filterByParams, TRUE), 0);

        [$query, $params] = $this->parseSql($sql, $request);

        if (!empty($filterByParams)) {
            $params = array_merge($params, $filterByParams);
        }

        error_log(print_r($sql, TRUE), 0);
        error_log(print_r($params, TRUE), 0);

        $startIndex = (int) $request->get('startIndex');
        $count      = (int) $request->get('count');

        $startIndex = $startIndex < 0 ? 0 : $startIndex;
        $limit      = $limit <= 0 ? 16 : $limit;
        $count      = $count >= 1 && $count <= $limit ? $count : $limit;

        $totalResults = (int) $connection->fetchColumn('SELECT COUNT(*) AS cnt FROM (' . $query . ') res', $params);

        $query = $connection->getDatabasePlatform()->modifyLimitQuery($query, $count, $startIndex);
        $data  = $connection->fetchAll($query, $params);

        $result = [
            'totalResults' => $totalResults,
            'itemsPerPage' => $count,
            'startIndex'   => $startIndex,
            'entry'        => $data,
        ];

        return $this->response->build(200, [], $result);
    }

    public function configure(BuilderInterface $builder, ElementFactoryInterface $elementFactory)
    {
        parent::configure($builder, $elementFactory);

        $builder->add($elementFactory->newInput('builtinFiltering', 'Builtin filtering', 'checkbox', 'Uncheck if you don\'t want builtin filtering parameters or your SQL statement contains custom filtering'));
        $builder->add($elementFactory->newTag('filteringColumns', 'Allowed filtering columns', 'Columns which are allowed to filter by (default is none)'));
        $builder->add($elementFactory->newInput('limit', 'Limit', 'number', 'The default limit of the result (default is 16)'));
    }

    private function addFilter(RequestInterface $request, string $query, $filteringColumns)
    {
        $filterBy    = strtolower($request->get('filterBy'));
        $filterOp    = strtolower($request->get('filterOp'));
        $filterValue = $request->get('filterValue');
        $count       = 0;

        $params = [];

        if (!empty($filterBy) && !empty($filterOp) && !empty($filterValue) && in_array($filterBy, $filteringColumns)) {
            switch ($filterOp) {
                case 'contains':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' LIKE :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = '%' . $filterValue . '%';
                    }
                    break;

                case 'equals':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' = :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue;
                    }
                    break;

                case 'startsWith':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' LIKE :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue . '%';
                    }
                    break;

                case 'present':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' IS NOT NULL', -1, $query);
                    break;
                
                case 'ne':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' != :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue . '%';
                    }
                    break;

                case 'gt':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' > :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue . '%';
                    }
                    break;

                case 'gte':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' >= :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue . '%';
                    }
                    break;
                
                case 'lt':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' < :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue . '%';
                    }
                    break;

                case 'lte':
                    $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' <= :filterValue', $query, -1, $count);
                    if ($count > 0) {
                        $params['filterValue'] = $filterValue . '%';
                    }
                    break;

                case 'between':
                    $b = $request->get('filterValueB');

                    if (!empty($b)) {
                        $query = preg_replace(self::FILTERBY_REGEX, $filterBy . ' BETWEEN :filterValueA AND :filterValueB', $query, -1, $count);
                        if ($count > 0) {
                            $params['filterValueA'] = $filterValue;
                            $params['filterValueB'] = $b;
                        }
                    } else {
                        $query = preg_replace(self::FILTERBY_REGEX, ' (1 = 1) ', $query);
                    }
                    break;
            }
        } else {
            $query = preg_replace(self::FILTERBY_REGEX, ' (1 = 1) ', $query);
        }
        return [
            $query,
            $params,
        ];
    }
}
