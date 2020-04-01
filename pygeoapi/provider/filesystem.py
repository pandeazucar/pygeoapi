# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2020 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import os
import rasterio

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError)

LOGGER = logging.getLogger(__name__)


class FileSystemProvider(BaseProvider):
    """filesystem Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.providers.elasticsearch_.ElasticsearchProvider
        """

        BaseProvider.__init__(self, provider_def)

        if not os.path.exists(self.data):
            msg = 'Directory does not exist: {}'.format(self.data)
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

    def get_data(self, path):
        root_link = None
        child_links = []

        data_path = os.path.join(self.data, path)
        data_path = self.data + path

        if '/' not in path:  # root
            root_link = './catalog.json'
        else:
            child_links.append({
                'rel': 'parent',
                'href': '../catalog.json'
            })

            depth = path.count('/')
            root_path = '/'.replace('/', '../' * depth, 1)
            root_link = os.path.join(root_path, 'catalog.json')

        content = {
            'links': [{
                'rel': 'root',
                'href': root_link
                }, {
                'rel': 'self',
                'href': './catalog.json'
                }
            ]
        }

        if not os.path.exists(data_path):
            msg = 'Directory does not exist: {}'.format(data_path)
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        if os.path.isdir(data_path):
            for dc in os.listdir(data_path):
                fullpath = os.path.join(data_path, dc)
                if os.path.isdir(fullpath):
                    child_links.append({
                        'rel': 'child',
                        'href': '{}/catalog.json'.format(dc)
                    })
                elif os.path.isfile(fullpath):
                    child_links.append({
                        'rel': 'item',
                        'href': '{}.json'.format(dc)
                    })
        elif os.path.isfile(data_path):
            content['id'] = data_path
            content['type'] = 'Feature'
            content['properties'] = {}

            d = rasterio.open(data_path)
            content['bbox'] = [
                d.bounds.left,
                d.bounds.bottom,
                d.bounds.right,
                d.bounds.top,
            ]
            content['geometry'] = {
                'type': 'Polygon',
                'coordinates': [[
                    [d.bounds.left, d.bounds.bottom],
                    [d.bounds.left, d.bounds.top],
                    [d.bounds.right, d.bounds.top],
                    [d.bounds.right, d.bounds.bottom],
                    [d.bounds.left, d.bounds.bottom]
                ]]
            }
            for k, v in d.tags(1).items():
                content['properties'][k] = v



        content['links'].extend(child_links)
        return content
