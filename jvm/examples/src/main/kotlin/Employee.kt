package io.andygrove.kquery.examples

import kotlin.system.measureTimeMillis

import io.andygrove.kquery.execution.ExecutionContext
import io.andygrove.kquery.logical.col
import io.andygrove.kquery.logical.format
import io.andygrove.kquery.optimizer.Optimizer

fun main() {
    val ctx = ExecutionContext(mapOf())
    val file = "jvm/testdata/employee.csv"

    val time =
            measureTimeMillis {
                val df = ctx.csv(file)
                        .project(listOf(col("first_name"), col("last_name")))

                val logicalPlan = df.logicalPlan()

                println("Logical Plan:\t${format(logicalPlan)}")

                val optimizedPlan = Optimizer().optimize(logicalPlan)
                println("Optimized Plan:\t${format(optimizedPlan)}")

                val results = ctx.execute(optimizedPlan)
                results.forEach {
                    println(it.schema)
                    println(it.toCSV())
                }
            }

    println("Query took ${time}ms")
}