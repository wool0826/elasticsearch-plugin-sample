package com.example

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.logging.LogManager
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

class FileLoggerActionFilter @Inject constructor(
    settings: Settings
) : ActionFilter.Simple() {
    private val log = LogManager.getLogger(javaClass)

    override fun apply(
        action: String,
        request: ActionRequest,
        listener: ActionListener<*>
    ): Boolean {
        if (request is IndexRequest) {
            log.info("Execute FileLoggerActionFilter. - requestClass: ${request.javaClass}")

            try {
                Files.write(
                    Paths.get("/my/data/log.json"),
                    request.toString().toByteArray(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
                )
            } catch (e: IOException) {
                log.error("Occurs errors while writing request to files.", e)
            }
        }

        return true
    }

    override fun order(): Int = Integer.MIN_VALUE
}
