import logging
from typing import Any, List, Iterable, Tuple, Dict
from google.cloud import datastore
from google.cloud.datastore import Entity


class DatastoreClient:

    def __init__(self, entity_kind: str) -> None:
        self.entity_kind = entity_kind
        logging.debug(f"Creating a Datastore client. Entity kind: {entity_kind}")
        self.client = datastore.Client()

    def get(self, id_or_name: str | int) -> Entity | None:
        key = self.client.key(self.entity_kind, id_or_name)
        return self.client.get(key)

    def batch_insert(self, batch_data: list[Dict]):
        entities = []
        for entity_dict in batch_data:
            entity = datastore.Entity(self.client.key(self.entity_kind, id=None))
            entity.update(entity_dict)
            entities.append(entity)
        self.client.put_multi(entities)
        return None

    def update(self, entry_id: int | str | None, content: Dict[Any, str], _to_exclude=()) -> Entity:

        entry = None

        if entry_id is None:
            key = self.client.key(self.entity_kind, id=None)
            entry = datastore.Entity(key, exclude_from_indexes=_to_exclude)
        else:
            entry = self.get(entry_id)
            if entry is None:
                key = self.client.key(self.entity_kind, entry_id)
                entry = datastore.Entity(key, exclude_from_indexes=_to_exclude)

        entry.update(content)
        self.client.put(entry)
        return entry

    def delete(self, id: int | str):
        key = self.client.key(self.entity_kind, id)
        self.client.delete(key)

    def delete_all(self, yes_i_im_sure: bool = False):

        if not yes_i_im_sure:
            return

        cursor, first_iteration = None, True
        while (cursor is not None or first_iteration):
            entities, new_cursor = self.list_batch(batch_size=50, cursor=cursor)
            for entity in entities:
                id = entity.key.name if entity.key.name is not None else \
                    entity.key.id
                key = self.client.key(self.entity_kind, id)
                self.client.delete(key)
            cursor = new_cursor
            first_iteration = False

    def list_all(self, order: str | None = None, filter: List[str] | None = None
                 ) -> Iterable:

        if order is not None and filter is not None:
            raise ValueError("Two filters at the same time is not supported.")

        query = self.client.query(kind=self.entity_kind)

        if order is not None:
            query.order = [order]

        if filter is not None:
            query.add_filter(*filter)

        return query.fetch()

    def list_batch(self, order: str | None = None, filter: List[str] | None = None,
                   batch_size: int = 50, cursor: bytes | None = None
                   ) -> Tuple[Iterable, bytes | None]:

        if order is not None and filter is not None:
            raise ValueError("Two filters at the same time is not supported.")

        query = self.client.query(kind=self.entity_kind)

        if order is not None:
            query.order = [order]

        if filter is not None:
            query.add_filter(*filter)

        query_iter = query.fetch(start_cursor=cursor, limit=batch_size)
        page = next(query_iter.pages)
        next_cursor = query_iter.next_page_token

        return page, next_cursor
