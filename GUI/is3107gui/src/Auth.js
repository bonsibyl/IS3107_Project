// import React, { useContext, useState, useEffect } from "react";
// import { supabase } from "./client";
// import { Session, User } from '@supabase/supabase-js';

// const AuthContext = React.createContext();

// export const AuthProvider = ({ children }) => {
//   const [user, setUser] = useState();
//   const [loading, setLoading] = useState(true);
//   const [session, setSession] = useState();

// const setData = async () => {
//     const { data ,error } = await supabase.auth.getSession();
//     if (error) throw error;
//     setSession(data)
//     setUser(session?.user)
//     //setUser(session?.user ?? null);
//     setLoading(false);
// };

//     // Listen for changes on auth state (logged in, signed out, etc.)
//     const { listener } = supabaseClient.auth.onAuthStateChange((_event, session) => {
//         setSession(session);
//         setUser(session?.user)
//         setLoading(false)
//     });

//     setData();

//     return () => {
//       //listener?.unsubscribe();
//       listener.subscription.unsubscribe();

//     };
//   }, []);

//   // Will be passed down to Signup, Login and Dashboard components
//   const value = {
//     signUp: (data) => supabase.auth.signUp(data),
//     signIn: (data) => supabase.auth.signIn(data),
//     signOut: () => supabase.auth.signOut(),
//     user,
//     userToken: (data) => supabase.auth.getUser("ACCESS_TOKEN_JWT"),
//   };

//   return (
//     <AuthContext.Provider value={value}>
//       {!loading && children}
//     </AuthContext.Provider>
//   );
// };

// export function useAuth() {
//   return useContext(AuthContext);
// }
