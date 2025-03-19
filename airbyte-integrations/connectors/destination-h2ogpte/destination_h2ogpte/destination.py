#
# Copyright (c) 2025 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, Mapping
import hashlib
import orjson
import logging
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models.airbyte_protocol_serializers import custom_type_resolver
from airbyte_cdk.exception_handler import init_uncaught_exception_handler
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStateMessage,
    AirbyteLogMessage,
    ConfiguredAirbyteCatalog,
    ConnectorSpecification,
    Status,
    Type,
    Level,
    AirbyteStateStats
)
from airbyte_cdk.models import Type as MessageType
from airbyte_cdk.models.file_transfer_record_message import AirbyteFileTransferRecordMessage
import urllib.parse
import uuid


from typing import Any, Dict, Iterable, List, Mapping, cast
from h2ogpte import H2OGPTE
from collections import defaultdict
import io
import tempfile
import os
from serpyco_rs import Serializer
from typing_extensions import override
from dataclasses import dataclass
from .patched_destination import PatchedDestination

logger = logging.getLogger("airbyte")


class H2OGPTEHelperClient:
    def __init__(self, config):
        self.sync_id = str(uuid.uuid4())
        self.logger = logging.getLogger("airbyte")
        self.streams_collection_ids = {}
        self.url = config["h2ogpte_url"]
        self.api_key = config["h2ogpte_api_key"]
        self.client = H2OGPTE(address=self.url, api_key=self.api_key)
        self.collection_name_prefix = ""
        self.destination_collection_id = None
        if "destination_collection_id" in config:
            self.destination_collection_id = config["destination_collection_id"]
        if "collection_name_prefix" in config:
            self.collection_name_prefix = config["collection_name_prefix"]

    def check(self):
        self.client.count_collections()

    def _getCollectionByName(self, collection_name):
        self.logger.info(f"Getting collection by name {self.collection_name_prefix + collection_name}")
        collections = self.client.list_recent_collections_filter(offset=0, limit=10, name_filter=self.collection_name_prefix + collection_name)
        for collection in collections:
            if collection.name == self.collection_name_prefix + collection_name:
                return collection.id
        return None
    
    def getOrCreateCollection(self, collection_name):
        if self.destination_collection_id is not None:
            self.logger.info(f"Using predefined destination collection {self.destination_collection_id}")
            return self.destination_collection_id
        self.logger.info(f"Getting or creating collection {self.collection_name_prefix + collection_name}")
        collection_id = self._getCollectionByName(collection_name)
        if collection_id:
            return collection_id
        return self.createCollection(collection_name)
    
    def createCollection(self, collection_name):
        self.logger.info(f"Creating collection {self.collection_name_prefix + collection_name}")
        return self.client.create_collection(name=self.collection_name_prefix + collection_name, description="Airbyte destination collection")

    def createCollectionForStreams(self, configured_catalog: ConfiguredAirbyteCatalog):
        streams = configured_catalog.streams
        self.streams_collection_ids = {}
        for stream in streams:
            collection_id = self.getOrCreateCollection(stream.stream.name)
            self.streams_collection_ids[stream.stream.name] = collection_id
            # if stream.destination_sync_mode == DestinationSyncMode.overwrite:
            #     self.wipeCollection(collection_id, stream.stream.name)

    def onBegin(self, configured_catalog: ConfiguredAirbyteCatalog):
        self.logger.info(f"Begin sync {self.sync_id}")
        self.createCollectionForStreams(configured_catalog)
    
    def onEnd(self, configured_catalog: ConfiguredAirbyteCatalog):
        self.cleanRemovedDocuments(configured_catalog)
        # self.dedublicateCollectionsIfNeeded(configured_catalog)

    def _documentMetadata(self, stream_name, source_file_name, hash):
        return { "source": "airbyte", "sync_id": self.sync_id, "stream": stream_name, "source_file_name": source_file_name, "hash": hash }

    def getDocuments(self, collection_id, stream_name, source_file_name, file_hash):
        metadata = { "source": "airbyte", "stream": stream_name, "source_file_name": source_file_name, "hash": file_hash }
        self.logger.info(f"Checking if document {source_file_name} exists with metadata {metadata}")
        matching_documents = self.client.list_documents_in_collection(collection_id, offset=0, limit=1000, metadata_filter=metadata)
        return matching_documents
    

    def updateDocumentsMetadata(self, documents, metadata):
        self.logger.info(f"Updating metadata {metadata} for documents {documents}")
        for document in documents:
            self.client.update_document_metadata(document.id, metadata)

    def getAllDocuments(self, collection_id, metadata={ "source": "airbyte" }):
        self.logger.info(f"Getting all documents in collection {collection_id}")
        document_ids = []
        limit = 1000
        offset = 0
        while True:
            documents = self.client.list_documents_in_collection(collection_id, offset=offset, limit=limit, metadata_filter=metadata)
            document_ids.extend([document.id for document in documents])
            if len(documents) < limit:
                break
            offset += limit
        return set(document_ids)
    
    def wipeCollection(self, collection_id, stream_name):
        self.logger.info(f"Wiping collection {collection_id}")
        document_ids = list( self.getAllDocuments(collection_id, metadata={ "source": "airbyte", "stream": stream_name }) )
        for i in range(0, len(document_ids), 100):
            self.client.delete_documents(document_ids[i:i+100])
        
    
    def setDocumentsMetadata(self, document_ids, metadata):
        self.logger.info(f"Setting metadata {metadata} for documents {document_ids}")
        for document_id in document_ids:
            self.client.update_document_metadata(document_id, metadata)

    def deleteDocument(self, collection_id, stream_name, source_file_name, file_hash):
        self.logger.info(f"Deleting document {source_file_name}")
        metadata = { "source": "airbyte", "stream": stream_name, "source_file_name": source_file_name }
        documents = self.client.list_documents_in_collection(collection_id, offset=0, limit=1000, metadata_filter=metadata)
        documents_for_deletion = [document.id for document in documents]
        if len(documents_for_deletion) == 0:
            self.logger.info(f"Document {source_file_name} not found")
            return False
        self.client.delete_documents(documents_for_deletion)
        self.logger.info(f"Document {source_file_name} deleted")
        return True
    
    def dedublicateCollection(self, collection_id):
        documents = self.client.list_documents_in_collection(collection_id, offset=0, limit=1000)
        document_names = []
        document_to_delete = []
        for document in documents:
            if document.name in document_names:
                document_to_delete.append(document.id)
            else:
                document_names.append(document.name)
        self.client.delete_documents(document_to_delete)
        
    def onRecord(self, record, configured_catalog: ConfiguredAirbyteCatalog):
        if record.stream in self.streams_collection_ids:
            collection_id = self.streams_collection_ids[record.stream]
            #check if record.data is json
            stream = next((stream for stream in configured_catalog.streams if stream.stream.name == record.stream), None)
            if stream is None:
                self.logger.error(f"Stream {record.stream} not found in configured catalog")
                return

            #check if record is AirbyteFileTransferRecordMessage
            if isinstance(record, AirbyteFileTransferRecordMessage):
                self.logger.info(f"Processing file transfer record {record}")
                if record.file and record.file["file_url"]:
                    file_url = record.file["file_url"]
                    source_file_url = record.file["source_file_url"]
                    file_hash = self.hashForFile(file_url)
                    metadata = self._documentMetadata(record.stream, source_file_url, file_hash)
                    with open(file_url, "rb") as f:
                        source_file_path = urllib.parse.urlparse(source_file_url).path
                        source_file_path = source_file_path[1:] if source_file_path.startswith("/") else source_file_path

                        exDocuments = self.getDocuments(collection_id, record.stream, source_file_url, file_hash)
                        if len(exDocuments) > 0:
                            self.logger.info(f"Document {source_file_path} already exists")
                            self.updateDocumentsMetadata(exDocuments, metadata)
                            return
                        
                        self.logger.info(f"Ingesting file {source_file_path}")
                        if len(source_file_path) == 0:
                            source_file_path = str(uuid.uuid4()) + ".txt"
                        upload_id = self.client.upload(source_file_path, f, uri=source_file_url)
                        self.client.ingest_uploads(collection_id, [upload_id, ], metadata={upload_id: metadata})
            else:
                record_string = ""
                if record.data:
                    if isinstance(record.data, str):
                        record_string = record.data
                    else:
                        try:
                            record_bytes = orjson.dumps(record.data, option=orjson.OPT_SORT_KEYS)
                            record_string = record_bytes.decode("utf-8")
                        except Exception as e:
                            self.logger.error(f"Error while serializing record data: {e}")
                            return

                record_hash = hashlib.md5(record_string.encode()).hexdigest()
                record_name = record_hash
                stream = next((stream for stream in configured_catalog.streams if stream.stream.name == record.stream), None)
                if stream is None:
                    return
                file_name = record_name + ".txt"
                metadata = self._documentMetadata(record.stream, file_name, record_hash)
                exDocuments = self.getDocuments(collection_id, record.stream, file_name, record_hash)
                if len(exDocuments) > 0:
                    self.logger.info(f"Document {record_name} already exists")
                    self.updateDocumentsMetadata(exDocuments, metadata)
                    return
                self.logger.info(f"Ingesting record {file_name}")
                self.client.ingest_from_plain_text(collection_id, record_string, file_name, metadata=metadata)
                
    # Ivan: bring it back instead of getAllDocuments approach after h2ogpte update >=1.6.28
    # def cleanRemovedDocuments(self, configured_catalog: ConfiguredAirbyteCatalog):
    #     self.logger.info(f"Cleaning removed documents")
    #     for stream in configured_catalog.streams:
    #         collection_id = self.streams_collection_ids[stream.stream.name]
    #         self.logger.info(f"Getting all documents in collection {collection_id}")
    #         limit = 1000
    #         offset = 0
    #         metadata={ "source": "airbyte", "stream": stream.stream.name }
    #         while True:
    #             documents = self.client.list_documents_in_collection(collection_id, offset=offset, limit=limit, metadata_filter=metadata)
    #             document_ids = []
    #             for document in documents:
    #                 if document.metadata_dict is None:
    #                     continue
    #                 if document.metadata_dict.get("sync_id") is None or document.metadata_dict.get("sync_id") == "":
    #                     continue
    #                 if document.metadata_dict.get("sync_id") != self.sync_id:
    #                     document_ids.append(document.id)
    #             for i in range(0, len(document_ids), 50):
    #                 self.logger.info(f"Deleting documents {document_ids[i:i+50]}")
    #                 self.client.delete_documents(document_ids[i:i+50])
    #             if len(documents) < limit:
    #                 break
    #             offset += limit
    #     self.logger.info(f"Cleaning removed documents done")

    def cleanRemovedDocuments(self, configured_catalog: ConfiguredAirbyteCatalog):
        self.logger.info(f"Cleaning removed documents")
        for stream in configured_catalog.streams:
            collection_id = self.streams_collection_ids[stream.stream.name]
            all_documents = self.getAllDocuments(collection_id, metadata={ "source": "airbyte", "stream": stream.stream.name })
            synced_documents = self.getAllDocuments(collection_id, metadata={ "source": "airbyte", "stream": stream.stream.name, "sync_id": self.sync_id })
            removed_documents = list( all_documents - synced_documents )
            for i in range(0, len(removed_documents), 50):
                self.logger.info(f"Deleting documents {removed_documents[i:i+50]}")
                self.client.delete_documents(removed_documents[i:i+50])
        self.logger.info(f"Cleaning removed documents done")
        
    def dedublicateCollectionsIfNeeded(self, configured_catalog: ConfiguredAirbyteCatalog):
        self.logger.info(f"Dedublicating collections")
        for stream in configured_catalog.streams:
            if stream.destination_sync_mode == DestinationSyncMode.append_dedup:
                collection_id = self.streams_collection_ids[stream.stream.name]
                self.dedublicateCollection(collection_id)
        self.logger.info(f"Dedublicating collections done")
    
    def hashForFile(self, filename):
        h  = hashlib.md5()
        b  = bytearray(128*1024)
        mv = memoryview(b)
        with open(filename, 'rb', buffering=0) as f:
            while n := f.readinto(mv):
                h.update(mv[:n])
        return h.hexdigest()
            

class DestinationH2OGPTE(PatchedDestination):

    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        legacy_state_messages: list[AirbyteMessage] = []
        records_since_last_checkpoint: dict[str, int] = defaultdict(int)


        client = H2OGPTEHelperClient(config=config)
        client.onBegin(configured_catalog)
        
        for message in input_messages:
            if message.type == Type.RECORD and message.record is not None:
                logger.info(f"Processing message {message}")
                logger.info(f"Processing record {message.record}")
                stream_name = message.record.stream
                client.onRecord(message.record, configured_catalog)
                records_since_last_checkpoint[stream_name] += 1

            if message.type == Type.STATE and message.state is not None:
                if message.state.stream is None:
                    logger.warning("Cannot process legacy state message, skipping.")
                    legacy_state_messages.append(message)
                    continue
                stream_name = message.state.stream.stream_descriptor.name
                message.state.destinationStats = AirbyteStateStats(
                    recordCount=records_since_last_checkpoint[stream_name],
                )
                records_since_last_checkpoint[stream_name] = 0

                yield message
            else:
                continue
        
        client.onEnd(configured_catalog)

        if legacy_state_messages:
            yield from legacy_state_messages

    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        client = H2OGPTEHelperClient(config=config)
        try:
            client.check()
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message="Error with exception:" + str(e))

