try:
    from pyspark.java_gateway import launch_gateway
    print("Launching Java gateway...")
    launch_gateway()
except Exception as e:
    print("❌ Exception:", e)
