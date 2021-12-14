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
use Fusio\Engine\ParametersInterface;
use Fusio\Engine\RequestInterface;
use PSX\Http\Exception as StatusCode;

/**
 * SqlQueryExecute
 *
 * @author  Mateusz Knapik <mateuszknapik@gmail.com>
 * @license http://www.gnu.org/licenses/agpl-3.0
 * @link    http://fusio-project.org
 */
class SqlQueryExecute extends SqlQueryAbstract
{
    public function getName()
    {
        return 'SQL-Query-Execute';
    }

    public function handle(RequestInterface $request, ParametersInterface $configuration, ContextInterface $context)
    {
        $connection = $this->getConnection($configuration);

        $sql = $configuration->get('sql');

        [$query, $params] = $this->parseSql($sql, $request);

        $stmt = $connection->prepare($query, $params);

        $result = $stmt->execute();

        if ($result != true) {
            throw new StatusCode\BadRequestException('Statement execution resulted in failure');
        }

        return $this->response->build(200, [], [
            'success' => true,
            'message' => 'Statement executed successfuly'
        ]);
    }
}
