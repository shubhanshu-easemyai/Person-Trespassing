# Person-Tresspassing multiple detection - new rdx; 0.0.6 media-server; 2.0.0 person-detection
from rdx import Connector, console_logger
from shapely.geometry import Polygon, Point
from typing import Any
import numpy as np
from PIL import Image
import mongoengine
import copy
import cv2
import os
import io

from models import *

connector = Connector(connection_type="kafka")
service_details = connector.app_settings()

logger = console_logger.setup_logger(name=service_details["SERVICE_NAME"])

logger.debug("mongodb://{}:{}@{}:{}/{}?authSource={}".format(
        service_details["SERVICE_SETTINGS"]["DATABASE_USERNAME"],
        service_details["SERVICE_SETTINGS"]["DATABASE_PASSWORD"],
        service_details["SERVICE_SETTINGS"]["DATABASE_HOST"],
        service_details["SERVICE_SETTINGS"]["DATABASE_PORT"],
        service_details["SERVICE_SETTINGS"]["DATABASE_NAME"],
        service_details["SERVICE_SETTINGS"]["DATABASE_NAME"],
    ))

mongoengine.connect(
    host="mongodb://{}:{}@{}:{}/{}?authSource={}".format(
        service_details["SERVICE_SETTINGS"]["DATABASE_USERNAME"],
        service_details["SERVICE_SETTINGS"]["DATABASE_PASSWORD"],
        service_details["SERVICE_SETTINGS"]["DATABASE_HOST"],
        service_details["SERVICE_SETTINGS"]["DATABASE_PORT"],
        service_details["SERVICE_SETTINGS"]["DATABASE_NAME"],
        service_details["SERVICE_SETTINGS"]["DATABASE_NAME"],
    ),
)

sources_list = []
polygons = []
loaded_camera_ids = {}
object_class_name = "person"
max_time_threshold = 0

detected_object_list = []

def fetch_default_settings(width, height):
    return {
        "ROI_settings": [
            {
                "roi_name": "roi1",
                "cords": {
                    "x1": 0,
                    "x2": width,
                    "x3": width,
                    "x4": 0,
                    "y1": 0,
                    "y2": 0,
                    "y3": height,
                    "y4": height,
                },
                "loi": [],
            }
        ],
    }


def load_configuration_settings(source_id, source_name, **kwargs):
    global sources_list, polygons, loaded_camera_ids, max_time_threshold
    try:
        source_info = SourceInfo.objects(
            source_id=source_id, source_name=source_name
        ).get()
        if source_id not in loaded_camera_ids:
            loaded_camera_ids[source_id] = {"source_name": source_name, "indexes": []}
        else:
            removed_items = 0
            first_index = 0
            for _id, _index in enumerate(loaded_camera_ids[source_id]["indexes"]):
                if _id == 0:
                    first_index = _index
                polygons.pop(_index - _id)
                sources_list.pop(_index - _id)
                removed_items += 1
            if removed_items != 0:
                for _source in loaded_camera_ids:
                    loaded_camera_ids[_source]["indexes"] = [x-removed_items if x >= first_index else x for x in loaded_camera_ids[_source]["indexes"]]

            loaded_camera_ids[source_id]["indexes"] = []
    except DoesNotExist:
        return

    usecase_settings = UsecaseParameters.objects(source_details=source_info).all()
    
    start_index = len(sources_list)

    try:
        for settings in usecase_settings:
            logger.debug(settings)
            for roi in settings.settings["ROI_settings"]:
                corners = []

                for i in range(int(len(roi["cords"].keys()) / 2)):
                    corners.append(
                        (
                            int(roi["cords"]["x{}".format(i + 1)]),
                            int(roi["cords"]["y{}".format(i + 1)]),
                        )
                    )

                polygons.append(Polygon(corners))
                sources_list.append(
                    {
                        "max_time_threshold": int(roi.get("max_time_threshold", 0)),
                        "source": settings.source_details,
                        "user": settings.user_details,
                        "roi": {"cords": roi["cords"], "roi_name": roi["roi_name"]},
                        "source_name": settings.source_details.source_name
                    }
                )
                max_time_threshold = int(roi.get("max_time_threshold", 0))
                loaded_camera_ids[source_id]["indexes"].append(start_index)
                start_index += 1
    except Exception as e:
        logger.debug(e)
        sources_list = []


def post_action(connector, index, alert_data, key, headers, transaction_id):
    data = {
        "task_name": "action",
        "func_kwargs": {
            "data": {
                "app_details": {
                    "app_name": alert_data["service_name"],
                    "tab_name": "general_settings",
                    "section_name": "action_on_person_trespassed",
                },
                "user_data": sources_list[index]["user"].payload(),
                "type": "alert",
                "alert_text": alert_data["output_data"][0]["alert_text"],
                "source_name": sources_list[index]["source"]["source_name"],
                "date_time": alert_data["date_time"],
            }
        },
    }

    general_settings = GeneralSettings.objects.get(
        output_name="action_on_person_trespassed",
        user_details=sources_list[index]["user"],
    )

    for action in general_settings.settings["actions"]:
        connector.produce_data(
            message=data,
            key=key,
            headers=headers,
            transaction_id=transaction_id,
            event_type="action",
            destination=action,
        )


def post_process(
    connector,
    storage_path,
    alert_schema,
    index,
    detected_objects_unprocessed,
    key,
    headers,
    transaction_id,
    **kwargs,
):

    medias = []

    metadata = connector.consume_from_source(
        topic=headers["topic"], partition=headers["partition"], offset=headers["offset"]
    )
    if metadata:
        nparr = np.frombuffer(metadata, np.uint8)
        raw_image = Image.open(io.BytesIO(nparr))
        image_rgb = np.array(raw_image)
        image_np_array = cv2.cvtColor(image_rgb, cv2.COLOR_RGB2BGR)
        image_name = "{}.jpg".format(
            datetime.datetime.utcnow().strftime("%d-%m-%Y_%H-%M-%S-%f")
        )
        sub_folder = os.path.join(
            datetime.datetime.utcnow().strftime("%Y-%m-%d"), "Image"
        )

        if not os.path.exists(os.path.join(storage_path, sub_folder)):
            os.makedirs(os.path.join(storage_path, sub_folder))

        image_path = os.path.join(storage_path, sub_folder, image_name)

        cv2.imwrite(image_path, image_np_array)

        detected_objects = []

        for detected_object in detected_objects_unprocessed:
            transformed_object = {
                "confidence": detected_object.pop("confidence"),
                "name": detected_object.pop("name"),
                "object_id": detected_object.pop("object_id"),
                "bounding_box": detected_object,
            }
            detected_objects.append(transformed_object)

        medias = [
            {
                "media_link": os.path.join(sub_folder, image_name),
                "media_width": headers["source_frame_width"],
                "media_height": headers["source_frame_height"],
                "media_type": "image",
                "roi_details": [copy.deepcopy(sources_list[index]["roi"])],
                "detections": copy.deepcopy(detected_objects), 
            }
        ]

        detected_object_list.clear() 

    alert_schema["group_name"] = headers["source_name"]
    alert_schema["sources"] = [sources_list[index]["source"].payload()]
    alert_schema["date_time"] = "{}Z".format(datetime.datetime.utcnow()).replace(
        " ", "T"
    )
    alert_schema["output_data"].append(
        {
            "transaction_id": transaction_id,
            "output": "person tresspassing detected",
            "priority": "medium",
            "alert_text": "person tresspassing detected in region {}".format(
                sources_list[index]["roi"]["roi_name"]
            ),
            "metadata": medias,
        }
    )
    connector.produce_data(
        message={
            "task_name": "alert",
            "metadata": alert_schema,
            **kwargs,
        },
        key=key,
        headers=headers,
        transaction_id=transaction_id,
        event_type="alert",
        destination="alert_management",
    )

    post_action(connector, index, alert_schema, key, headers, transaction_id)


class AppSourceSettingsHandler:
    def __init__(self, connector: Connector) -> None:
        self.connector = connector

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        func_name = "{}_settings".format(kwds["type"])
        if hasattr(self, func_name) and callable(func := getattr(self, func_name)):
            try:
                func(**kwds)
            except Exception as e:
                logger.debug(e)

    def link_source_settings(self, sources: dict, users: dict, **kwargs):
        for group_name, group_sources in sources.items():
            for source_details in group_sources:
                try:
                    source_info = SourceInfo.objects.get(
                        source_id=source_details["source_id"]
                    )
                except DoesNotExist:
                    source_info = SourceInfo(**source_details)
                    source_info.save()

                _source_details = {}
                for k, v in source_details.items():
                    if k != "source_id":
                        _source_details["set__{}".format(k)] = v
                source_info.update(**_source_details)

                try:
                    user_details = UserInfo.objects.get(user_id=users["user_id"])
                except DoesNotExist:
                    user_details = UserInfo(**users)
                    user_details.save()

                try:
                    usecase_parameters = UsecaseParameters.objects.get(
                        source_details=source_info, user_details=user_details
                    )
                    usecase_parameters.settings = (
                        kwargs["settings"]
                        if "settings" in kwargs
                        else fetch_default_settings(
                            source_details["resolution"][0],
                            source_details["resolution"][1],
                        )
                    )
                except DoesNotExist:
                    usecase_parameters = UsecaseParameters(
                        source_details=source_info,
                        user_details=user_details,
                        settings=kwargs["settings"]
                        if "settings" in kwargs
                        else fetch_default_settings(
                            source_details["resolution"][0],
                            source_details["resolution"][1],
                        ),
                    )

                usecase_parameters.save()
                load_configuration_settings(**source_info.payload())
        return "success"

    def unlink_source_settings(self, sources: dict, users: dict, **kwargs):
        try:
            for group_name, group_sources in sources.items():
                for source_details in group_sources:
                    source_info = SourceInfo.objects.get(
                        source_id=source_details["source_id"]
                    )
                    user_info = UserInfo.objects.get(user_id=users["user_id"])

                    usecase_parameters = UsecaseParameters.objects.get(
                        source_details=source_info, user_details=user_info
                    )
                    usecase_parameters.delete()
                    load_configuration_settings(**source_info.payload())
            return "success"
        except DoesNotExist:
            pass

    def update_source_settings(self, sources: dict, users: dict, **kwargs):
        new_resolution = []
        prev_resolution = []
        try:
            for group_name, group_sources in sources.items():
                for source_details in group_sources:
                    _source_details = {}
                    for k, v in source_details.items():
                        _source_details["set__{}".format(k)] = v
                        if k == "resolution":
                            new_resolution = copy.deepcopy(v)

                    source_info = SourceInfo.objects.get(
                        source_id=source_details["source_id"]
                    )
                    prev_resolution = copy.deepcopy(source_info.resolution)
                    source_info.update(**_source_details)

                    user_details = UserInfo.objects.get(user_id=users["user_id"])

                    if (
                        new_resolution[0] != prev_resolution[0]
                        or new_resolution[1] != prev_resolution[1]
                    ):
                        usecase_parameters = UsecaseParameters.objects.get(
                            source_details=source_info, user_details=user_details
                        )

                        updated_roi_settings = []
                        for roi_settings in usecase_parameters.settings["ROI_settings"]:
                            for k, v in roi_settings["cords"].items():
                                if k.count("x") != 0:
                                    roi_settings["cords"][k] = int(
                                        v / prev_resolution[0] * new_resolution[0]
                                    )
                                else:
                                    roi_settings["cords"][k] = int(
                                        v / prev_resolution[1] * new_resolution[1]
                                    )
                            updated_roi_settings.append(copy.deepcopy(roi_settings))
                        usecase_parameters.settings[
                            "ROI_settings"
                        ] = updated_roi_settings
                        usecase_parameters.save()

                    load_configuration_settings(**source_info.payload())
            return "success"
        except DoesNotExist:
            pass


class AppGeneralSettingsHandler:
    def __init__(self, connector: Connector) -> None:
        self.connector = connector

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        func_name = "{}_general_settings".format(kwds["type"])
        if hasattr(self, func_name) and callable(func := getattr(self, func_name)):
            try:
                func(**kwds)
            except Exception as e:
                logger.debug(e)

    def send_data_to_server(self, session_id, task_name, data):
        self.connector.produce_data(
            message={
                "task_name": task_name,
                "func_kwargs": {
                    "session_id": session_id,
                    **data,
                },
            },
            destination="socket_server",
            event_type="general_setting",
        )

    def get_general_settings(self, session_id, tab_name, user_data, **kwds):
        try:
            user_details = UserInfo.objects(**user_data).get()
            if tab_name == "general_settings":
                general_settings = GeneralSettings.objects(
                    user_details=user_details
                ).get()
                self.send_data_to_server(
                    session_id=session_id,
                    task_name="get",
                    data={general_settings.output_name: general_settings.settings},
                )
        except Exception as e:
            logger.debug(e)

    def post_general_settings(self, session_id, tab_name, settings, user_data, **kwds):
        try:
            try:
                user_details = UserInfo.objects(**user_data).get()
            except DoesNotExist:
                user_details = UserInfo(**user_data)
                user_details.save()
                
            if tab_name == "general_settings":
                for output_name, setting in settings.items():
                    try:
                        general_settings = GeneralSettings.objects(
                            user_details=user_details, output_name=output_name
                        ).get()
                    except DoesNotExist:
                        general_settings = GeneralSettings(
                            user_details=user_details, output_name=output_name
                        )
                    general_settings.settings = setting
                    general_settings.save()
                
                self.send_data_to_server(
                    session_id=session_id,
                    task_name="post",
                    data={"detail": "success"},
                )
        except Exception as e:
            logger.debug(e)

    def reset_general_settings(self, session_id, tab_name, user_data, **kwds):
        try:
            user_details = UserInfo.objects(**user_data).get()
            if tab_name == "general_settings":
                general_settings = GeneralSettings.objects.get(
                    user_details=user_details
                )
                general_settings.delete()

                self.send_data_to_server(
                    session_id=session_id,
                    task_name="reset",
                    data={"detail": "success"},
                )
        except Exception as e:
            logger.debug(e)


class AppConfigurationSettingsHandler:
    def __init__(self, connector: Connector) -> None:
        self.connector = connector

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        func_name = "{}_configuration_settings".format(kwds["type"])
        if hasattr(self, func_name) and callable(func := getattr(self, func_name)):
            try:
                func(**kwds)
            except Exception as e:
                logger.debug(e)

    def send_data_to_server(self, session_id, task_name, data):
        self.connector.produce_data(
            message={
                "task_name": task_name,
                "func_kwargs": {
                    "session_id": session_id,
                    **data,
                },
            },
            destination="socket_server",
            event_type="configuration_settings",
        )

    def get_configuration_settings(
        self, session_id, tab_name, user_data, source_details, **kwds
    ):
        try:
            source_info = SourceInfo.objects(**source_details).get()
            user_details = UserInfo.objects(**user_data).get()
            if tab_name == "configuration_settings":
                usecase_parameters = UsecaseParameters.objects.get(
                    source_details=source_info, user_details=user_details
                )
                self.send_data_to_server(
                    session_id=session_id,
                    task_name="get",
                    data=usecase_parameters.settings,
                )
        except Exception as e:
            logger.debug(e)

    def post_configuration_settings(
        self, session_id, tab_name, settings, user_data, source_details, **kwds
    ):
        try:
            source_info = SourceInfo.objects(**source_details).get()
            user_details = UserInfo.objects(**user_data).get()
            if tab_name == "configuration_settings":
                usecase_parameters = UsecaseParameters.objects.get(
                    source_details=source_info, user_details=user_details
                )
                usecase_parameters.settings = settings
                usecase_parameters.save()
                load_configuration_settings(**source_info.payload())
                self.send_data_to_server(
                    session_id=session_id,
                    task_name="post",
                    data={"detail": "success"},
                )
        except Exception as e:
            logger.debug(e)

    def reset_configuration_settings(
        self, session_id, tab_name, user_data, source_details, **kwds
    ):
        try:
            source_info = SourceInfo.objects(**source_details).get()
            user_details = UserInfo.objects(**user_data).get()
            if tab_name == "configuration_settings":
                usecase_parameters = UsecaseParameters.objects.get(
                    source_details=source_info, user_details=user_details
                )
                usecase_parameters.settings = fetch_default_settings(
                    source_info.resolution[0],
                    source_info.resolution[1],
                )
                usecase_parameters.save()
                self.send_data_to_server(
                    session_id=session_id,
                    task_name="reset",
                    data={"detail": "success"},
                )
                load_configuration_settings(**source_info.payload())
        except Exception as e:
            logger.debug(e)


class DataProcessor:
    def __init__(self, connector: Connector, service_details: dict) -> None:
        global detected_object_list
        self.connector = connector
        self.object_tracker = {}

        self.alert_metadata = {
            "service_name": service_details["SERVICE_NAME"],
            "service_tags": service_details["SERVICE_SETTINGS"]["SERVICE_TAGS"].split(
                ","
            ),
            "sources": [],
            "target_service": [],
            "output_data": [],
            "date_time": None,
        }

        self.image_storage_path = os.path.join(os.getcwd(), "custom_data")
        if "SERVICE_MOUNTS" in service_details:
            self.image_storage_path = service_details["SERVICE_MOUNTS"]["output_media"]

    def process_data(self, data, **kwargs):
        detected_object_list.clear()
        transaction_id = kwargs.pop("transaction_id")
        key = kwargs.pop("key")
        source_details = kwargs.pop("headers")

        try:
            camera_present = loaded_camera_ids[source_details["source_id"]]
        except KeyError:
            load_configuration_settings(**source_details)

        for detected_object in copy.deepcopy(data["detections"]):
            if detected_object["name"] == object_class_name and detected_object["confidence"] >= 0.5:
                x_coordinate = (detected_object["x1"] + detected_object["x2"]) // 2
                y_coordinate = (detected_object["y1"] + detected_object["y4"]) // 2

                for _id in loaded_camera_ids[source_details["source_id"]]["indexes"]:
                    if Point(x_coordinate, y_coordinate).within(polygons[_id]):
                        object_id = "{}_{}_{}".format(
                            source_details["source_id"],
                            sources_list[_id]["roi"]["roi_name"],
                            detected_object["object_id"],
                        )
                        if object_id not in self.object_tracker:
                            self.object_tracker[object_id] = {
                                "created": datetime.datetime.utcnow(),
                                "alert": False,
                            }
                        elif not self.object_tracker[object_id]["alert"]:
                            if (
                                datetime.datetime.utcnow()
                                - self.object_tracker[object_id]["created"]
                            ).seconds > max_time_threshold:
                                self.object_tracker[object_id]["alert"] = True
                                detected_object_list.append(detected_object)
                                
        if len(detected_object_list) > 0:
            post_process(
                connector=connector,
                storage_path=self.image_storage_path,
                alert_schema=copy.deepcopy(self.alert_metadata),
                index=_id,
                detected_objects_unprocessed=detected_object_list,
                key=key,
                headers=source_details,
                transaction_id=transaction_id,
                **data,
            )


@connector.consume_events
def fetch_events(data: dict, *args, **kwargs):
    logger.debug(data)
    if data["data"]["task_name"] == "source_group_settings":
        source_settings_handler = AppSourceSettingsHandler(connector=connector)
        source_settings_handler(**data["data"]["func_kwargs"]["data"])
    elif data["data"]["task_name"] == "general_settings":
        general_settings_handler = AppGeneralSettingsHandler(connector=connector)
        general_settings_handler(**data["data"]["func_kwargs"]["data"])
    elif data["data"]["task_name"] == "configuration_settings":
        configuration_settings_handler = AppConfigurationSettingsHandler(
            connector=connector
        )
        configuration_settings_handler(**data["data"]["func_kwargs"]["data"])


dataProcessor = DataProcessor(connector=connector, service_details=service_details)


@connector.consume_data
def fetch_metadata(data: dict, *args, **kwargs):
    try:
        dataProcessor.process_data(**data)
    except Exception as e:
        logger.debug(e)


connector.run()
