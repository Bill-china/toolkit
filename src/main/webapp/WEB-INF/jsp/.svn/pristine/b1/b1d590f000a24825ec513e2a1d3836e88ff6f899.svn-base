<?php
/**
*配置文件管理目录
*@author hujinglin-ps@360.cn
*/
class JConfig
{
    public static $config;

    /**
     * Returns the configuration array for the requested group.  See
     * [configuration files](/config) for more information.
     *
     *     // Get all the configuration in config/database.php
     *     $config = Jconfig::item('database');
     *
     *     // Get only the default connection configuration
     *     $default = Jconfig::item('database.default')
     *
     *     // Get only the hostname of the default connection
     *     $host = Jconfig::item('database.default.connection.hostname')
     *
     * @param   string   group name
     * @return  Config
     */
    public static function item($group)
    {

        if (strpos($group, '.') !== FALSE)
        {
            // Split the config group and path
            list ($group, $path) = explode('.', $group, 2);
        }

        if (!isset($config[$group]))
        {
            $config[$group] = new CConfiguration(Yii::getPathOfAlias('application.config') . '/'.$group.'.php');

        }

        if (isset($path))
        {
            return self::arrPath($config[$group], $path, NULL, '.');
        }
        else
        {
            return $config[$group];
        }
    }

      /**
     * Gets a value from an array using a dot separated path.
     *
     * @param   array   array to search
     * @param   mixed   key path string (delimiter separated) or array of keys
     * @param   mixed   default value if the path is not set
     * @param   string  key path delimiter
     * @return  mixed
     */
    public static function arrPath($array, $path, $default = NULL, $delimiter = NULL)
    {
        if ( ! self::is_array($array))
        {
            // This is not an array!
            return $default;
        }

        if (is_array($path))
        {
            // The path has already been separated into keys
            $keys = $path;
        }
        else
        {
            if (array_key_exists($path, $array))
            {
                // No need to do extra processing
                return $array[$path];
            }

            if ($delimiter === NULL)
            {
                // Use the default delimiter
                $delimiter = '.';
            }

            // Remove starting delimiters and spaces
            $path = ltrim($path, "{$delimiter} ");

            // Remove ending delimiters, spaces, and wildcards
            $path = rtrim($path, "{$delimiter} *");

            // Split the keys by delimiter
            $keys = explode($delimiter, $path);
        }

        do
        {
            $key = array_shift($keys);

            if (ctype_digit($key))
            {
                // Make the key an integer
                $key = (int) $key;
            }

            if (isset($array[$key]))
            {
                if ($keys)
                {
                    if (self::is_array($array[$key]))
                    {
                        // Dig down into the next part of the path
                        $array = $array[$key];
                    }
                    else
                    {
                        // Unable to dig deeper
                        break;
                    }
                }
                else
                {
                    // Found the path requested
                    return $array[$key];
                }
            }
            elseif ($key === '*')
            {
                // Handle wildcards

                $values = array();
                foreach ($array as $arr)
                {
                    if ($value = self::path($arr, implode('.', $keys)))
                    {
                        $values[] = $value;
                    }
                }

                if ($values)
                {
                    // Found the values requested
                    return $values;
                }
                else
                {
                    // Unable to dig deeper
                    break;
                }
            }
            else
            {
                // Unable to dig deeper
                break;
            }
        }
        while ($keys);

        // Unable to find the value requested
        return $default;
    }

    /**
     * Test if a value is an array with an additional check for array-like objects.
     *
     * @param   mixed    value to check
     * @return  boolean
     */
    public static function is_array($value)
    {
        if (is_array($value))
        {
            // Definitely an array
            return TRUE;
        }
        else
        {
            // Possibly a Traversable object, functionally the same as an array
            return (is_object($value) AND $value instanceof Traversable);
        }
    }

}
