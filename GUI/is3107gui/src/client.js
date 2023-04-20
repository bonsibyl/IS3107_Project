import { createClient } from "@supabase/supabase-js";

const supabaseUrl = "https://ldlurtxycltozsvzhlfi.supabase.co";

const supabaseAnonKey =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxkbHVydHh5Y2x0b3pzdnpobGZpIiwicm9sZSI6ImFub24iLCJpYXQiOjE2ODE3MjE2NDMsImV4cCI6MTk5NzI5NzY0M30.__3IVOq4nWYYa9BMRN8kfCW1gyvKkB536EQUs2i60-o";
export const supabase = createClient(supabaseUrl, supabaseAnonKey);
