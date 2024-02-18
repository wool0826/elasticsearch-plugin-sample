package com.example

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.TransportAction
import org.elasticsearch.client.internal.Client
import org.elasticsearch.client.internal.node.NodeClient
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.node.DiscoveryNodes
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.io.stream.NamedWriteableRegistry
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.settings.ClusterSettings
import org.elasticsearch.common.settings.IndexScopedSettings
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.settings.SettingsFilter
import org.elasticsearch.env.Environment
import org.elasticsearch.env.NodeEnvironment
import org.elasticsearch.logging.LogManager
import org.elasticsearch.plugins.ActionPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.repositories.RepositoriesService
import org.elasticsearch.rest.BaseRestHandler
import org.elasticsearch.rest.BaseRestHandler.RestChannelConsumer
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestHandler
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.ScriptService
import org.elasticsearch.tasks.Task
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.tracing.Tracer
import org.elasticsearch.transport.TransportService
import org.elasticsearch.watcher.ResourceWatcherService
import org.elasticsearch.xcontent.NamedXContentRegistry
import org.elasticsearch.xcontent.ToXContent
import org.elasticsearch.xcontent.XContentBuilder
import java.util.function.Supplier

const val NAME: String = "cluster:manage/example/myplugin"

object MyPluginActionType : ActionType<MyPluginResponse>(NAME, ::MyPluginResponse)

class MyPlugin(settings: Settings) : Plugin(), ActionPlugin {
    private val log = LogManager.getLogger(javaClass)
    private val fileLoggerFilter = FileLoggerActionFilter(settings)

    init {
        log.info("init!")
    }

    override fun getSettings(): MutableList<Setting<*>> {
        log.info("getSettings")
        return mutableListOf(
            Setting.simpleString("hello.greetings", Setting.Property.Dynamic, Setting.Property.NodeScope)
        )
    }

    override fun getActions(): MutableList<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return mutableListOf(
            ActionPlugin.ActionHandler(
                MyPluginActionType,
                TransportMyPluginAction::class.java
            )
        )
    }

    override fun getRestHandlers(
        settings: Settings?,
        restController: RestController?,
        clusterSettings: ClusterSettings?,
        indexScopedSettings: IndexScopedSettings?,
        settingsFilter: SettingsFilter?,
        indexNameExpressionResolver: IndexNameExpressionResolver?,
        nodesInCluster: Supplier<DiscoveryNodes>?
    ): MutableList<RestHandler> {
        return mutableListOf(RestMyPluginAction())
    }

    override fun createComponents(
        client: Client?,
        clusterService: ClusterService?,
        threadPool: ThreadPool?,
        resourceWatcherService: ResourceWatcherService?,
        scriptService: ScriptService?,
        xContentRegistry: NamedXContentRegistry?,
        environment: Environment?,
        nodeEnvironment: NodeEnvironment?,
        namedWriteableRegistry: NamedWriteableRegistry?,
        indexNameExpressionResolver: IndexNameExpressionResolver?,
        repositoriesServiceSupplier: Supplier<RepositoriesService>?,
        tracer: Tracer?
    ): MutableCollection<Any> {
        return mutableListOf(fileLoggerFilter)
    }

    override fun getActionFilters(): MutableList<ActionFilter> {
        return mutableListOf(fileLoggerFilter)
    }
}

class MyPluginRequest : ActionRequest {
    val name: String

    constructor(name: String) {
        this.name = name
    }

    constructor(input: StreamInput) {
        this.name = input.readString()
    }

    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString("ActionRequest writeTo!")
    }

    override fun validate(): ActionRequestValidationException? {
        return if (name.isEmpty()) {
            ActionRequestValidationException().apply {
                addValidationError("name parameter must not be empty")
            }
        } else {
            null
        }
    }
}

class MyPluginResponse : ActionResponse, ToXContent {
    private val greetings: String
    private val name: String

    constructor(greetings: String, name: String) {
        this.greetings = greetings
        this.name = name
    }

    constructor(input: StreamInput) : super(input) {
        this.greetings = input.readString()
        this.name = input.readString()
    }

    override fun writeTo(out: StreamOutput) {
        out.writeString(greetings)
        out.writeString(name)
    }

    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field("message", "$greetings $name")
        builder.endObject()

        return builder
    }
}

class TransportMyPluginAction @Inject constructor(
    actionFilters: ActionFilters,
    transportService: TransportService,
    private val settings: Settings
) : TransportAction<MyPluginRequest, MyPluginResponse>(NAME, actionFilters, transportService.taskManager) {

    override fun doExecute(task: Task, request: MyPluginRequest, listener: ActionListener<MyPluginResponse>) {
        try {
            val greetings = settings.get("hello.greetings", "hello")
            listener.onResponse(MyPluginResponse(greetings, request.name))
        } catch (e: Exception) {
            listener.onFailure(e)
        }
    }
}

class RestMyPluginAction : BaseRestHandler() {
    override fun routes(): MutableList<RestHandler.Route> =
        mutableListOf(RestHandler.Route(RestRequest.Method.GET, "/_hello"))

    override fun getName(): String = "my_plugin_action"

    override fun prepareRequest(request: RestRequest, client: NodeClient): RestChannelConsumer {
        val name = request.param("name", "world")
        val actionRequest = MyPluginRequest(name)

        return RestChannelConsumer { channel ->
            client.execute(MyPluginActionType, actionRequest, object : ActionListener<MyPluginResponse> {
                override fun onResponse(response: MyPluginResponse) {
                    channel.sendResponse(DefaultRestResponse.success(response, channel))
                }

                override fun onFailure(e: Exception) {
                    channel.sendResponse(DefaultRestResponse.error(e))
                }
            })
        }
    }
}

class DefaultRestResponse private constructor(
    private val restStatus: RestStatus,
    private val content: String
) : RestResponse(restStatus, content) {
    override fun contentType(): String = "application/json; charset=UTF-8"
    override fun content(): BytesReference = BytesArray(content)
    override fun status(): RestStatus = restStatus

    companion object {
        fun success(
            toXContent: ToXContent,
            channel: RestChannel,
            restStatus: RestStatus = RestStatus.OK
        ): DefaultRestResponse {
            val xContentBuilder = channel.newBuilder()
            toXContent.toXContent(xContentBuilder, channel.request())

            return DefaultRestResponse(restStatus, Strings.toString(xContentBuilder))
        }

        fun error(
            e: Throwable,
            restStatus: RestStatus = RestStatus.INTERNAL_SERVER_ERROR
        ): DefaultRestResponse {
            return DefaultRestResponse(restStatus, buildJsonContent(e))
        }

        private fun buildJsonContent(e: Throwable) = """{"errorMessage": "$e"}"""
    }
}
